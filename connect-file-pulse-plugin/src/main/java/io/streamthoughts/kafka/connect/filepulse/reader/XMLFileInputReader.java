/*
 * Copyright 2019 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import net.sf.saxon.lib.NamespaceConstant;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
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

        private final NodeList nodes;

        private final int totalRecords;

        private int position = 0;

        XMLFileInputIterator(final XMLFileInputReaderConfig configs,
                             final IteratorManager iteratorManager,
                             final FileContext context) {
            super(iteratorManager, context);
            System.setProperty("javax.xml.xpath.XPathFactory:"+  NamespaceConstant.OBJECT_MODEL_SAXON,
                    "net.sf.saxon.xpath.XPathFactoryImpl");
            try (FileInputStream is = new FileInputStream(context.file())) {

                DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = builderFactory.newDocumentBuilder();
                Document document = builder.parse(new InputSource(is));

                final XPathFactory xPathFactory = XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);
                XPath expression = xPathFactory.newXPath();
                XPathExpression e = expression.compile(configs.xpathQuery());

                nodes = (NodeList) e.evaluate(document, XPathConstants.NODESET);
                totalRecords = nodes.getLength();
            } catch (XPathExpressionException e) {
                throw new ReaderException(
                    "Error happened while compiling XPath query '" + configs.xpathQuery() + "'", e);
            } catch (IOException e) {
                throw new ReaderException(
                    "Error happened while reading source file '" + context + "'", e);
            } catch (Exception e) {
                throw new ReaderException(
                    "Unexpected error happened while initializing 'XMLFileInputIterator'", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void seekTo(final SourceOffset offset) {
            position = (int)offset.position();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RecordsIterable<FileRecord<TypedStruct>> next() {
            final Node item = nodes.item(position);
            if (item == null) {
                return RecordsIterable.empty();
            }

            position++;

            final TypedStruct struct = Node2StructConverter.convertNodeObjectTree(item);
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

    private static class Node2StructConverter {

        private static TypedStruct convertNodeObjectTree(final Node node) throws DOMException {
            Objects.requireNonNull(node, "node cannot be null");
            TypedStruct container = TypedStruct.struct();
            readAllNodeAttributes(node, container);
            for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
                Optional<?> optional = readNodeObject(child);
                if (optional.isPresent()) {
                    final Object nodeValue = optional.get();
                    final String nodeName = isTextNode(child) ? determineNodeName(node) : determineNodeName(child);
                    container = enrichStructWithObject(container, nodeName, nodeValue);

                }
            }
            return container;
        }

        private static TypedStruct enrichStructWithObject(final TypedStruct container,
                                                          final String nodeName,
                                                          final Object nodeValue) {
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
            } else {
                value = TypedValue.any(nodeValue);
            }
            return container.put(nodeName, value);
        }

        private static Optional<?> readNodeObject(final Node node) {
            if (isNewLineNodeElement(node)) {
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
                    return Optional.of(convertNodeObjectTree(node));
                }
            }
            throw new ReaderException("Unsupported node type '" + node.getNodeType() + "'");
        }

        private static Optional<String> peekChildNodeTextContent(final Node node) {
            if (isSingleChildNode(node)) {
                Node child = node.getFirstChild();
                if (isTextNode(child)) {
                    // Text content can be an empty string.
                    return Optional.of(child.getTextContent());
                }
            }
            return Optional.empty();
        }

        private static boolean isSingleChildNode(final Node node) {
            return node.hasChildNodes() && node.getChildNodes().getLength() == 1;
        }

        private static boolean isTextNode(final Node n) {
            return isNodeOfType(n, Node.TEXT_NODE);
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

        private static boolean isNewLineNodeElement(final Node node) {
            return node != null && isTextNode(node) && node.getTextContent().trim().isEmpty();
        }

        private static String determineNodeName(final Node node) {
            return node.getLocalName() != null ? node.getLocalName() : node.getNodeName();
        }
    }
}
