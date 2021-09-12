/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.xml;

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.FieldPaths;
import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;

/**
 * Utility class to convert a {@link Node} object into {@link TypedStruct}.
 */
public final class XMLNodeToStructConverter implements Function<Node, TypedStruct> {

    private static final Logger LOG = LoggerFactory.getLogger(XMLNodeToStructConverter.class);

    private static final String DEFAULT_TEXT_NODE_FIELD_NAME = "value";
    private static final Pattern NAME_INVALID_CHARACTERS = Pattern.compile("[.\\-]");
    private static final String NAME_INVALID_CHARACTER_REPLACEMENT = "_";

    private boolean excludeEmptyElement = false;

    private boolean isTypeInferenceEnabled = false;

    private FieldPaths forceArrayFields = FieldPaths.empty();

    public XMLNodeToStructConverter setExcludeEmptyElement(boolean excludeEmptyElement) {
        this.excludeEmptyElement = excludeEmptyElement;
        return this;
    }

    public XMLNodeToStructConverter setForceArrayFields(final FieldPaths forceArrayFields) {
        this.forceArrayFields = forceArrayFields;
        return this;
    }

    public XMLNodeToStructConverter setTypeInferenceEnabled(final boolean isTypeInferenceEnabled) {
        this.isTypeInferenceEnabled = isTypeInferenceEnabled;
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
        return convertObjectTree(node, forceArrayFields).getStruct();
    }

    private TypedValue convertObjectTree(final Node node,
                                         final FieldPaths forceArrayFields) {
        Objects.requireNonNull(node, "'node' cannot be null");

        final String nodeName = determineNodeName(node);
        final FieldPaths currentForceArrayFields = nodeName.equals("#document")
                ? forceArrayFields :
                forceArrayFields.next(sanitizeNodeName(nodeName));

        // Create a new Struct container object for holding all node elements, i.e., child nodes and attributes.
        TypedStruct container = TypedStruct.create();
        addAllNodeAttributes(container, node.getAttributes());
        for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
            // Text nodes always return #text" as the node name, so it's best to use the parent node name instead.
            final String childNodeName = isTextNode(child) ? nodeName : determineNodeName(child);
            Optional<TypedValue> optional = readObjectNodeValue(child, currentForceArrayFields);
            if (optional.isPresent()) {
                final TypedValue nodeValue = optional.get();
                if (excludeEmptyElement &&
                        nodeValue.type() == Type.STRUCT &&
                        nodeValue.isEmpty()) {
                    LOG.debug("Empty XML element excluded: '{}'", node.getNodeName());
                    continue;
                }
                final boolean forceElementAsArray = currentForceArrayFields.anyMatches(childNodeName);
                container = enrichStructWithObject(container, childNodeName, nodeValue, forceElementAsArray);
            }
        }
        return TypedValue.struct(container);
    }

    private Optional<TypedValue> readObjectNodeValue(final Node node,
                                                     final FieldPaths forceArrayFields) {
        if (isWhitespaceOrNewLineNodeElement(node)) {
            return Optional.empty();
        }

        if (isTextNode(node)) {
            final String text = node.getNodeValue();
            final TypedValue data = toTypedValue(text);
            return readTextNodeValue(node, data);
        }

        if (isElementNode(node)) {
            // Check if Element node contains only a single CDataNode.
            final Optional<String> childTextContent = peekChildCDataNodeTextValue(node);
            if (childTextContent.isPresent()) {
                final String text = childTextContent.get();
                final TypedValue data = toTypedValue(text);
                return readTextNodeValue(node, data);
            }
            return Optional.of(convertObjectTree(node, forceArrayFields));
        }

        throw new ReaderException("Unsupported node type '" + node.getNodeType() + "'");
    }

    private TypedValue toTypedValue(final String text) {
        return isTypeInferenceEnabled ? TypedValue.parse(text) : TypedValue.string(text);
    }

    private static Optional<TypedValue> readTextNodeValue(final Node node, final TypedValue data) {
        // Check if TextNode as no attribute
        final NamedNodeMap attributes = node.getAttributes();
        if (attributes == null || attributes.getLength() == 0) {
            return Optional.of(data);
        }

        // Else, create a Struct container
        final TypedStruct container = TypedStruct.create();
        addAllNodeAttributes(container, attributes);
        container.put(DEFAULT_TEXT_NODE_FIELD_NAME, data);
        return Optional.of(TypedValue.struct(container));
    }

    private static TypedStruct enrichStructWithObject(final TypedStruct container,
                                                      final String nodeName,
                                                      final TypedValue nodeObject,
                                                      final boolean forceElementAsArray) {
        final TypedValue value;
        if (container.has(nodeName)) {
            value = handleRepeatedElementsAsArray(container, nodeName, nodeObject);
        } else if (forceElementAsArray) {
            List<Object> array = new LinkedList<>();
            array.add(nodeObject.value());
            value = TypedValue.array(array, nodeObject.schema());
        } else {
            value = nodeObject;
        }
        return container.put(nodeName, value);
    }

    private static TypedValue handleRepeatedElementsAsArray(final TypedStruct container,
                                                            final String nodeName,
                                                            final TypedValue nodeObject) {
        final TypedField field = container.field(nodeName);
        final Schema mergedValueSchema;

        List<Object> array;
        if (field.type() == Type.ARRAY) {
            mergedValueSchema = ((ArraySchema)field.schema()).valueSchema().merge(nodeObject.schema());
            array = new LinkedList<>(container.getArray(nodeName));
        } else {
            // Auto-convert duplicates into an Array containing the previous and new elements.
            mergedValueSchema = field.schema().merge(nodeObject.schema());
            array = new LinkedList<>();
            array.add(container.get(nodeName).value());
        }

        // Add the new object to the array.
        array.add(nodeObject.value());

        // Make sure to convert all array values to the target array value-schema.
        final Type valueType = mergedValueSchema.type();
        if (valueType.isPrimitive()) {
            array = array
                    .stream()
                    .map(valueType::convert)
                    .collect(Collectors.toList());
        }
        return TypedValue.array(array, mergedValueSchema);
    }

    private static Optional<String> peekChildCDataNodeTextValue(final Node node) {
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
        return sanitizeNodeName(name);
    }

    private static String sanitizeNodeName(final String name) {
        return NAME_INVALID_CHARACTERS
                .matcher(name)
                .replaceAll(NAME_INVALID_CHARACTER_REPLACEMENT);
    }
}
