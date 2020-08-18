/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.SchemaSupplier;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import net.sf.saxon.lib.NamespaceConstant;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord.DEFAULT_MESSAGE_FIELD;
import static java.util.Collections.singletonList;

/**
 * This {@link FileInputReader} can be used for reading XML source files.
 */
public class XMLFileInputReader extends AbstractFileInputReader {

    private XMLFileInputReaderConfig configs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.configs = new XMLFileInputReaderConfig(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileContext context,
                                                                     final IteratorManager iteratorManager) {
        return new XMLFileInputIterator(configs, iteratorManager, context);
    }

    private static class XMLFileInputIterator extends AbstractFileInputIterator<TypedStruct> {

        private final XMLFileInputReaderConfig config;

        private final Object xpathResult;

        private final int totalRecords;

        private int position = 0;

        private final ResultType type;

        private enum ResultType {NODE_SET, STRING }

        XMLFileInputIterator(final XMLFileInputReaderConfig config,
                             final IteratorManager iteratorManager,
                             final FileContext context) {
            super(iteratorManager, context);

            this.config = config;
            System.setProperty(
                "javax.xml.xpath.XPathFactory:"+  NamespaceConstant.OBJECT_MODEL_SAXON,
                "net.sf.saxon.xpath.XPathFactoryImpl"
            );

            final QName qName = new QName("http://www.w3.org/1999/XSL/Transform", config.resultType());

            try (FileInputStream is = new FileInputStream(context.file())) {

                DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
                builderFactory.setIgnoringElementContentWhitespace(true);
                builderFactory.setIgnoringComments(true);
                DocumentBuilder builder = builderFactory.newDocumentBuilder();
                Document document = builder.parse(new InputSource(is));

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
            }

            else if (XPathConstants.STRING.equals(qName)) {
                type = ResultType.STRING;
                totalRecords = 1;
            }
            else {
                throw new ReaderException("Unsupported result type '" + config.resultType() + "'");
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void seekTo(final SourceOffset offset) {
            Objects.requireNonNull(offset, "offset can't be null");
            if (offset.position() != -1) {
                this.position = (int)offset.position();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RecordsIterable<FileRecord<TypedStruct>> next() {

            if (type == ResultType.NODE_SET) {
                final Node item = ((NodeList)xpathResult).item(position);

                if (item == null) return RecordsIterable.empty();

                try {
                    return incrementAndGet(Node2StructConverter.convertNodeObjectTree(item, config.forceArrayFields()));
                } catch (Exception e) {
                    throw new ReaderException("Fail to convert XML document to connect struct object: " + context, e);
                }
            }

            if (type == ResultType.STRING) {
                return incrementAndGet(TypedStruct.create().put(DEFAULT_MESSAGE_FIELD, (String) xpathResult));
            }

            throw new ReaderException("Unsupported result type '" + type + "'");
        }

        private RecordsIterable<FileRecord<TypedStruct>> incrementAndGet(final TypedStruct struct) {
            position++;
            return RecordsIterable.of(new TypedFileRecord(new XMLRecordOffset(position), struct));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return position < totalRecords;
        }
    }

    /**
     * Utility class to convert a {@link Node} object into {@link TypedStruct}.
     */
    private static class Node2StructConverter {

        /**
         * Converts the given {@link Node} object tree into a new new {@link TypedStruct} instance.
         *
         * @param node      the {@link Node} object tree to convert.
         * @return          the new {@link TypedStruct} instance.
         */
        private static TypedStruct convertNodeObjectTree(final Node node, final List<String> forceArrayFields) {
            Objects.requireNonNull(node, "node cannot be null");

            TypedStruct container = TypedStruct.create();
            readAllNodeAttributes(node, container);
            for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
                Optional<?> optional = readNodeObject(child, forceArrayFields);
                if (optional.isPresent()) {
                    final Object nodeValue = optional.get();
                    final String nodeName = isTextNode(child) ? determineNodeName(node) : determineNodeName(child);
                    final boolean isArray = forceArrayFields.contains(nodeName);
                    container = enrichStructWithObject(container, nodeName, nodeValue, isArray);

                }
            }
            return container;
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
                    value = TypedValue.array(array, ((ArraySchema)field.schema()).valueSchema());
                } else {
                    List<Object> array = new LinkedList<>();
                    array.add(container.get(nodeName).value());
                    array.add(nodeValue);
                    value = TypedValue.array(array, field.schema());
                }
            } else if(forceArrayField) {
                List<Object> array = new LinkedList<>();
                array.add(nodeValue);
                value = TypedValue.array(array, SchemaSupplier.lazy(nodeValue).get());
            } else {
                value = TypedValue.any(nodeValue);
            }
            return container.put(nodeName, value);
        }

        private static Optional<?> readNodeObject(final Node node, final List<String> forceArrayFields) {
            if (isWhitespaceOrNewLineNodeElement(node)) {
                return Optional.empty();
            }

            if (isTextNode(node)) {
                return Optional.of(node.getNodeValue());
            }

            if (isElementNode(node)) {
                Optional<String> childTextContent = peekChildNodeTextContent(node);
                if (childTextContent.isPresent()) {
                    return childTextContent;
                } else {
                    return Optional.of(convertNodeObjectTree(node, forceArrayFields));
                }
            }
            throw new ReaderException("Unsupported node type '" + node.getNodeType() + "'");
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

        private static void readAllNodeAttributes(final Node node, final TypedStruct values) {
            final NamedNodeMap attributes = node.getAttributes();
            if (attributes == null) return;

            for (int i = 0; i < attributes.getLength(); i++) {
                Node attr = attributes.item(i);
                String attrName = determineNodeName(attr);
                if (isNotXmlNamespace(attr)) {
                    values.put(attrName, attr.getNodeValue());
                }
            }
        }

        private static boolean isNotXmlNamespace(final Node node) {
            return !XMLConstants.XMLNS_ATTRIBUTE.equalsIgnoreCase(node.getPrefix()) ;
        }

        private static boolean isWhitespaceOrNewLineNodeElement(final Node node) {
            return node != null && isTextNode(node) && node.getTextContent().trim().isEmpty();
        }

        private static String determineNodeName(final Node node) {
            return node.getLocalName() != null ? node.getLocalName() : node.getNodeName();
        }
    }
}
