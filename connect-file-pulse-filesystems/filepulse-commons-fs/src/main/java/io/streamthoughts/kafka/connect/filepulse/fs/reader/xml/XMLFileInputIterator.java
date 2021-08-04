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

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.FieldPaths;
import io.streamthoughts.kafka.connect.filepulse.data.SchemaSupplier;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IndexRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.ManagedFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import net.sf.saxon.lib.NamespaceConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord.DEFAULT_MESSAGE_FIELD;
import static java.util.Collections.singletonList;

public class XMLFileInputIterator extends ManagedFileInputIterator<TypedStruct> {

    private static final Logger LOG = LoggerFactory.getLogger(XMLFileInputIterator.class);

    private final Object xpathResult;

    private final int totalRecords;

    private final ResultType type;

    private enum ResultType {NODE_SET, STRING}

    private final Node2StructConverter converter;

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

        this.converter = new Node2StructConverter()
                .setExcludeEmptyElement(config.isEmptyElementExcluded())
                .setForceArrayFields(FieldPaths.from(config.forceArrayFields()));

        System.setProperty(
                "javax.xml.xpath.XPathFactory:" + NamespaceConstant.OBJECT_MODEL_SAXON,
                "net.sf.saxon.xpath.XPathFactoryImpl"
        );

        final QName qName = new QName("http://www.w3.org/1999/XSL/Transform", config.resultType());

        try (stream) {

            final DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
            builderFactory.setIgnoringElementContentWhitespace(true);
            builderFactory.setIgnoringComments(true);
            builderFactory.setNamespaceAware(config.isNamespaceAwareEnabled());
            builderFactory.setValidating(config.isValidatingEnabled());

            final DocumentBuilder builder = builderFactory.newDocumentBuilder();

            if (config.isValidatingEnabled()) {
                builder.setErrorHandler(new ErrorHandler() {
                    @Override
                    public void warning(final SAXParseException e) {
                        LOG.warn("Handled XML parser warning on file {}. Error: {}",
                            objectMeta.uri(),
                            e.getLocalizedMessage()
                        );
                    }

                    @Override
                    public void error(final SAXParseException e) {
                        LOG.warn("Handled XML parser error on file {}. Error: {}",
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
            }
            final Document document = builder.parse(new InputSource(stream));

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

    /**
     * Utility class to convert a {@link Node} object into {@link TypedStruct}.
     */
    private static final class Node2StructConverter implements Function<Node, TypedStruct> {

        private static final Logger LOG = LoggerFactory.getLogger(Node2StructConverter.class);

        private static final String DEFAULT_TEXT_NODE_FIELD_NAME = "value";
        private static final Pattern NAME_INVALID_CHARACTERS = Pattern.compile("[.\\-]");
        private static final String NAME_INVALID_CHARACTER_REPLACEMENT = "_";

        private boolean excludeEmptyElement;

        private FieldPaths forceArrayFields = FieldPaths.empty();

        public Node2StructConverter setExcludeEmptyElement(boolean excludeEmptyElement) {
            this.excludeEmptyElement = excludeEmptyElement;
            return this;
        }

        public Node2StructConverter setForceArrayFields(final FieldPaths forceArrayFields) {
            this.forceArrayFields = forceArrayFields;
            return this;
        }

        /**
         * Converts the given {@link Node} object tree into a new new {@link TypedStruct} instance.
         *
         * @param node the {@link Node} object tree to convert.
         * @return the new {@link TypedStruct} instance.
         */
        @Override
        public TypedStruct apply(final Node node) {
            return convertObjectTree(node, forceArrayFields);
        }

        private TypedStruct convertObjectTree(final Node node,
                                              final FieldPaths forceArrayFields) {
            Objects.requireNonNull(node, "node cannot be null");
            String nodeName = determineNodeName(node);
            final FieldPaths currentForceArrayFields = nodeName.equals("#document")
                    ? forceArrayFields :
                    forceArrayFields.next(nodeName);

            TypedStruct container = TypedStruct.create();
            addAllNodeAttributes(container, node.getAttributes());
            for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
                final String childNodeName = isTextNode(child) ? nodeName : determineNodeName(child);
                Optional<?> optional = readNodeObject(child, currentForceArrayFields);
                if (optional.isPresent()) {
                    Object nodeValue = optional.get();
                    final boolean isArray = currentForceArrayFields.anyMatches(childNodeName);
                    container = enrichStructWithObject(container, childNodeName, nodeValue, isArray);

                }
            }
            return container;
        }

        private Optional<?> readNodeObject(final Node node,
                                           final FieldPaths forceArrayFields) {
            if (isWhitespaceOrNewLineNodeElement(node)) {
                return Optional.empty();
            }

            if (isTextNode(node)) {
                return readTextNode(node, node.getNodeValue());
            }

            if (isElementNode(node)) {

                if (excludeEmptyElement && isEmptyNode(node)) {
                    LOG.debug("Empty XML element excluded: '{}'", node.getNodeName());
                    return Optional.empty();
                }

                Optional<String> childTextContent = peekChildNodeTextContent(node);
                if (childTextContent.isPresent()) {
                    return readTextNode(node, childTextContent.get());
                } else {
                    return Optional.of(convertObjectTree(node, forceArrayFields));
                }
            }
            throw new ReaderException("Unsupported node type '" + node.getNodeType() + "'");
        }

        private static TypedStruct enrichStructWithObject(final TypedStruct container,
                                                          final String nodeName,
                                                          final Object nodeValue,
                                                          final boolean forceArrayField) {
            TypedValue value;
            if (container.has(nodeName)) {
                final TypedField field = container.field(nodeName);
                if (field.type() == Type.ARRAY) {
                    List<Object> array = container.getArray(nodeName);
                    array.add(nodeValue);
                    value = TypedValue.array(array, ((ArraySchema) field.schema()).valueSchema());
                } else {
                    List<Object> array = new LinkedList<>();
                    array.add(container.get(nodeName).value());
                    array.add(nodeValue);
                    value = TypedValue.array(array, field.schema());
                }
            } else if (forceArrayField) {
                List<Object> array = new LinkedList<>();
                array.add(nodeValue);
                value = TypedValue.array(array, SchemaSupplier.lazy(nodeValue).get());
            } else {
                value = TypedValue.any(nodeValue);
            }
            return container.put(nodeName, value);
        }

        private static boolean isEmptyNode(final Node node) {
            return node.getChildNodes().getLength() == 0 &&
                    node.getAttributes().getLength() == 0;
        }

        private static Optional<?> readTextNode(final Node node,
                                                final String text) {
            final NamedNodeMap attributes = node.getAttributes();
            if (attributes != null && attributes.getLength() > 0) {
                final TypedStruct container = TypedStruct.create();
                addAllNodeAttributes(container, attributes);
                container.put(DEFAULT_TEXT_NODE_FIELD_NAME, text);
                return Optional.of(container);
            }
            return Optional.of(text);
        }

        private static Optional<String> peekChildNodeTextContent(final Node node) {
            if (!node.hasChildNodes()) return Optional.empty();

            final List<Node> nonNewLineNodes = collectAllNotNewLineNodes(node.getChildNodes());

            if (nonNewLineNodes.size() == 1) {
                final Node child = nonNewLineNodes.get(0);
                if (isTextNode(child)) {
                    // Text content can be an empty string.
                    return Optional.of(child.getTextContent());
                }
            }

            return Optional.empty();
        }

        private static List<Node> collectAllNotNewLineNodes(final NodeList nodes) {
            if (nodes.getLength() == 1) {
                return singletonList(nodes.item(0));
            }

            return IntStream.range(0, nodes.getLength())
                    .mapToObj(nodes::item)
                    .filter(it -> !isWhitespaceOrNewLineNodeElement(it))
                    .collect(Collectors.toList());
        }

        private static boolean isTextNode(final Node n) {
            return isNodeOfType(n, Node.TEXT_NODE) ||
                    isNodeOfType(n, Node.CDATA_SECTION_NODE);
        }

        private static boolean isElementNode(final Node n) {
            return isNodeOfType(n, Node.ELEMENT_NODE);
        }

        private static boolean isNodeOfType(final Node node, int textNode) {
            return node.getNodeType() == textNode;
        }

        private static void addAllNodeAttributes(final TypedStruct struct,
                                                 final NamedNodeMap attributes) {
            if (attributes == null) return;

            for (int i = 0; i < attributes.getLength(); i++) {
                Attr attr = (Attr) attributes.item(i);
                String attrName = determineNodeName(attr);
                if (isNotXmlNamespace(attr)) {
                    struct.put(attrName, attr.getNodeValue());
                }
            }
        }

        private static boolean isNotXmlNamespace(final Node node) {
            return !XMLConstants.XMLNS_ATTRIBUTE.equalsIgnoreCase(node.getPrefix());
        }

        private static boolean isWhitespaceOrNewLineNodeElement(final Node node) {
            return node != null && isTextNode(node) && node.getTextContent().trim().isEmpty();
        }

        private static String determineNodeName(final Node node) {
            final String name = node.getLocalName() != null ? node.getLocalName() : node.getNodeName();
            return NAME_INVALID_CHARACTERS
                    .matcher(name)
                    .replaceAll(NAME_INVALID_CHARACTER_REPLACEMENT);
        }

    }
}
