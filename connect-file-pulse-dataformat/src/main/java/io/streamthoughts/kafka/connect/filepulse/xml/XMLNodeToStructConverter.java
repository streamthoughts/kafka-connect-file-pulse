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

import static java.util.Collections.singletonList;

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.FieldPaths;
import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.xml.XMLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Utility class to convert a {@link Node} object into {@link TypedStruct}.
 */
public final class XMLNodeToStructConverter implements Function<Node, TypedStruct> {

    private static final Logger LOG = LoggerFactory.getLogger(XMLNodeToStructConverter.class);

    private static final Pattern FIELD_CHARACTERS_REGEX_PATTERN_DEFAULT = Pattern.compile("[.\\-]");
    private static final String FIELD_CHARACTERS_STRING_REPLACEMENT_DEFAULT = "_";

    private static final String XML_TEXT_NODE_VALUE_FIELD_NAME_DEFAULT = "value";
    public static final String DOCUMENT_NODE_NAME = "#document";

    private boolean excludeEmptyElement = false;

    private boolean excludeAllAttributes = false;

    private boolean isTypeInferenceEnabled = false;

    private String contentFieldName = XML_TEXT_NODE_VALUE_FIELD_NAME_DEFAULT;

    private Set<String> excludeAttributesInNamespaces = Collections.emptySet();

    private String attributePrefix = "";

    private FieldPaths forceArrayFields = FieldPaths.empty();

    private FieldPaths forceContentFields = FieldPaths.empty();

    private Pattern fieldCharactersRegexPattern = FIELD_CHARACTERS_REGEX_PATTERN_DEFAULT;

    private String fieldCharactersStringReplacement = FIELD_CHARACTERS_STRING_REPLACEMENT_DEFAULT;

    public XMLNodeToStructConverter setContentFieldName(String textNodeValueFieldName) {
        this.contentFieldName = textNodeValueFieldName;
        return this;
    }

    public XMLNodeToStructConverter setExcludeEmptyElement(boolean excludeEmptyElement) {
        this.excludeEmptyElement = excludeEmptyElement;
        return this;
    }

    public XMLNodeToStructConverter setExcludeAllAttributes(boolean excludeAllAttributes) {
        this.excludeAllAttributes = excludeAllAttributes;
        return this;
    }

    public XMLNodeToStructConverter setFieldCharactersRegexPattern(Pattern fieldCharactersRegexPattern) {
        this.fieldCharactersRegexPattern = fieldCharactersRegexPattern;
        return this;
    }

    public XMLNodeToStructConverter setFieldCharactersStringReplacement(String fieldCharactersStringReplacement) {
        this.fieldCharactersStringReplacement = fieldCharactersStringReplacement;
        return this;
    }

    public XMLNodeToStructConverter setExcludeAttributesInNamespaces(final Set<String> excludeAttributesInNamespaces) {
        this.excludeAttributesInNamespaces = Collections.unmodifiableSet(excludeAttributesInNamespaces);
        return this;
    }

    public XMLNodeToStructConverter setAttributePrefix(final String attributePrefix) {
        this.attributePrefix = attributePrefix;
        return this;
    }

    public XMLNodeToStructConverter setForceArrayFields(final FieldPaths forceArrayFields) {
        this.forceArrayFields = forceArrayFields;
        return this;
    }

    public XMLNodeToStructConverter setForceContentFields(final FieldPaths forceContentFields) {
        this.forceContentFields = forceContentFields;
        return this;
    }

    public XMLNodeToStructConverter setTypeInferenceEnabled(final boolean isTypeInferenceEnabled) {
        this.isTypeInferenceEnabled = isTypeInferenceEnabled;
        return this;
    }

    /**
     * Converts the given {@link Node} object tree into a new {@link TypedStruct} instance.
     *
     * @param node the {@link Node} object tree to convert.
     * @return the new {@link TypedStruct} instance.
     */
    @Override
    public TypedStruct apply(final Node node) {
        return convertObjectTree(node, forceArrayFields, forceContentFields).getStruct();
    }

    private TypedValue convertObjectTree(final Node node,
                                         final FieldPaths forceArrayFields,
                                         final FieldPaths forceContentFields) {
        Objects.requireNonNull(node, "'node' cannot be null");

        final String nodeName = determineNodeName(node);
        final FieldPaths currentForceArrayFields = nodeName.equals(DOCUMENT_NODE_NAME)
                ? forceArrayFields :
                forceArrayFields.next(sanitizeNodeName(nodeName));

        final FieldPaths currentForceContentFields = nodeName.equals(DOCUMENT_NODE_NAME)
                ? forceContentFields :
                forceContentFields.next(sanitizeNodeName(nodeName));

        // Create a new Struct container object for holding all node elements, i.e., child nodes and attributes.
        TypedStruct container = TypedStruct.create();
        getNotExcludedNodeAttributes(node).forEach(container::put);
        for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
            // Text nodes always return #text as the node name, so it's best to use the parent node name instead.
            final String childNodeName = isTextNode(child) ? nodeName : determineNodeName(child);
            Optional<TypedValue> optional = readObjectNodeValue(
                    child,
                    childNodeName,
                    currentForceArrayFields,
                    currentForceContentFields
            );
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
                                                     final String nodeName,
                                                     final FieldPaths forceArrayFields,
                                                     final FieldPaths forceContentFields) {
        if (node == null) return Optional.empty();

        if (isWhitespaceOrNewLineNodeElement(node) ||
            isNodeOfType(node, Node.DOCUMENT_TYPE_NODE)) {
            return Optional.empty();
        }

        if (isTextNode(node)) {
            final String text = node.getNodeValue();
            final TypedValue data = toTypedValue(text);

            return readTextNodeValue(node, data, forceContentFields.anyMatches(nodeName));
        }

        if (isElementNode(node)) {
            // Check if Element node contains only a single CDataNode.
            final Optional<String> childTextContent = peekChildCDataNodeTextValue(node);
            if (childTextContent.isPresent()) {
                final String text = childTextContent.get();
                final TypedValue data = toTypedValue(text);
                return readTextNodeValue(node, data, forceContentFields.anyMatches(nodeName));
            }
            return Optional.of(convertObjectTree(node, forceArrayFields, forceContentFields));
        }

        throw new ReaderException("Unsupported node type '" + node.getNodeType() + "'");
    }

    private TypedValue toTypedValue(final String text) {
        return isTypeInferenceEnabled ? TypedValue.parse(text) : TypedValue.string(text);
    }

    private Optional<TypedValue> readTextNodeValue(final Node node,
                                                   final TypedValue data,
                                                   boolean forceContentField) {
        // Check if TextNode as no attribute
        final Map<String, String> attributes = getNotExcludedNodeAttributes(node);
        if (attributes.isEmpty() && !forceContentField) {
            return Optional.of(data);
        }

        // Else, create a Struct container
        final TypedStruct container = TypedStruct.create();
        attributes.forEach(container::put);
        container.put(contentFieldName, data);
        return Optional.of(TypedValue.struct(container));
    }

    private Map<String, String> getNotExcludedNodeAttributes(final Node node) {
        final NamedNodeMap nodeMap = node.getAttributes();
        if (excludeAllAttributes || nodeMap == null || nodeMap.getLength() == 0)  {
            return Collections.emptyMap();
        }

        final Map<String, String> attributes = new HashMap<>();
        for (int i = 0; i < nodeMap.getLength(); i++) {
            Attr attr = (Attr) nodeMap.item(i);
            String namespaceURI = attr.getNamespaceURI();
            if (namespaceURI == null || !excludeAttributesInNamespaces.contains(namespaceURI)) {
                String attrName = determineNodeName(attr);
                if (isNotXmlNamespace(attr)) {
                    attributes.put(attributePrefix + attrName, attr.getNodeValue());
                }
            }
        }
        return attributes;
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
            if (isWhitespaceOrNewLineNodeElement(child))
                return Optional.empty();
            if (isTextNode(child))
                return Optional.of(child.getTextContent());
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
        return node != null && node.getNodeType() == textNode;
    }

    private static boolean isNotXmlNamespace(final Node node) {
        return !XMLConstants.XMLNS_ATTRIBUTE.equalsIgnoreCase(node.getPrefix());
    }

    private static boolean isWhitespaceOrNewLineNodeElement(final Node node) {
        return node != null && isTextNode(node) && StringUtils.isBlank(node.getTextContent());
    }

    private String determineNodeName(final Node node) {
        final String name = node.getLocalName() != null ? node.getLocalName() : node.getNodeName();
        return sanitizeNodeName(name);
    }

    private String sanitizeNodeName(final String name) {
        return fieldCharactersRegexPattern
                .matcher(name)
                .replaceAll(fieldCharactersStringReplacement);
    }
}
