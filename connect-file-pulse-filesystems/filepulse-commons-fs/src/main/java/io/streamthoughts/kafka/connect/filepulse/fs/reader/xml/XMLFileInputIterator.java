/*
 * Copyright 2019-2021 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.xml;

import static io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord.DEFAULT_MESSAGE_FIELD;

import io.streamthoughts.kafka.connect.filepulse.data.FieldPaths;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IndexRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.ManagedFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import io.streamthoughts.kafka.connect.filepulse.xml.XMLDocumentReader;
import io.streamthoughts.kafka.connect.filepulse.xml.XMLNodeToStructConverter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import net.sf.saxon.lib.NamespaceConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXParseException;

public class XMLFileInputIterator extends ManagedFileInputIterator<TypedStruct> {

    private static final Logger LOG = LoggerFactory.getLogger(XMLFileInputIterator.class);

    private final Object xpathResult;

    private final int totalRecords;

    private final ResultType type;

    private enum ResultType {NODE_SET, STRING}

    private final XMLNodeToStructConverter converter;

    private int position = 0;

    /**
     * Creates a new {@link XMLFileInputIterator} instance.
     *
     * @param config          the {@link XMLFileInputReaderConfig}.
     * @param iteratorManager the {@link IteratorManager}.
     * @param objectMeta      the {@link FileObjectMeta} of the file to process.
     * @param stream          the {@link InputStream} of the file to process.
     */
    public XMLFileInputIterator(final XMLFileInputReaderConfig config,
                                final IteratorManager iteratorManager,
                                final FileObjectMeta objectMeta,
                                final InputStream stream) {
        super(objectMeta, iteratorManager);

        this.converter = new XMLNodeToStructConverter()
                .setExcludeEmptyElement(config.isEmptyElementExcluded())
                .setExcludeAllAttributes(config.isNodeAttributesExcluded())
                .setExcludeAttributesInNamespaces(config.getExcludeNodeAttributesInNamespaces())
                .setForceArrayFields(FieldPaths.from(config.forceArrayFields()))
                .setForceContentFields(FieldPaths.from(config.getForceContentFields()))
                .setTypeInferenceEnabled(config.isDataTypeInferenceEnabled())
                .setContentFieldName(config.getContentFieldName())
                .setFieldCharactersRegexPattern(config.getXmlFieldCharactersRegexPattern())
                .setFieldCharactersStringReplacement(config.getXmlFieldCharactersStringReplacement())
                .setAttributePrefix(config.getAttributePrefix());

        final QName qName = new QName("http://www.w3.org/1999/XSL/Transform", config.resultType());

        try (stream) {

            final XMLDocumentReader reader = new XMLDocumentReader(
                    config.isNamespaceAwareEnabled(),
                    config.isValidatingEnabled()
            );

            final Document document;
            if (config.isValidatingEnabled()) {
                document = reader.parse(stream, new ErrorHandler() {
                    @Override
                    public void warning(final SAXParseException e) {
                        LOG.warn(
                                "Handled XML parser warning on file {}. Error: {}",
                                objectMeta.uri(),
                                e.getLocalizedMessage()
                        );
                    }

                    @Override
                    public void error(final SAXParseException e) {
                        LOG.warn(
                                "Handled XML parser error on file {}. Error: {}",
                                objectMeta.uri(),
                                e.getLocalizedMessage()
                        );
                    }

                    @Override
                    public void fatalError(final SAXParseException e) {
                        throw new ReaderException(
                                "Handled XML parser fatal error on file '" + objectMeta.uri() + "'",
                                e
                        );
                    }
                });
            } else {
                document = reader.parse(stream);
            }

            final XPathFactory xPathFactory = XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);
            XPath expression = xPathFactory.newXPath();
            XPathExpression e = expression.compile(config.xpathQuery());

            xpathResult = e.evaluate(document, qName);

        } catch (XPathExpressionException e) {
            throw new ReaderException(
                    "Cannot compile XPath expression '" + config.xpathQuery() + "'", e);
        } catch (IOException e) {
            throw new ReaderException(
                    "Error happened while reading source file '" + context + "'", e);
        } catch (Exception e) {
            throw new ReaderException(
                    "Unexpected error happened while initializing 'XMLFileInputIterator'", e);
        }

        if (XPathConstants.NODESET.equals(qName)) {
            type = ResultType.NODE_SET;
            totalRecords = ((NodeList) xpathResult).getLength();
        } else if (XPathConstants.STRING.equals(qName)) {
            type = ResultType.STRING;
            totalRecords = 1;
        } else {
            throw new ReaderException("Unsupported result type '" + config.resultType() + "'");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        Objects.requireNonNull(offset, "offset can't be null");
        if (offset.position() != -1) {
            this.position = (int) offset.position();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {

        if (type == ResultType.NODE_SET) {
            final Node item = ((NodeList) xpathResult).item(position);

            if (item == null) return RecordsIterable.empty();

            try {
                return incrementAndGet(converter.apply(item));
            } catch (Exception e) {
                throw new ReaderException("Failed to convert XML document to connect struct object: " + context, e);
            }
        }

        if (type == ResultType.STRING) {
            return incrementAndGet(TypedStruct.create().put(DEFAULT_MESSAGE_FIELD, (String) xpathResult));
        }

        throw new ReaderException("Unsupported result type '" + type + "'");
    }

    private RecordsIterable<FileRecord<TypedStruct>> incrementAndGet(final TypedStruct struct) {
        position++;
        return RecordsIterable.of(new TypedFileRecord(new IndexRecordOffset(position), struct));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return position < totalRecords;
    }

}
