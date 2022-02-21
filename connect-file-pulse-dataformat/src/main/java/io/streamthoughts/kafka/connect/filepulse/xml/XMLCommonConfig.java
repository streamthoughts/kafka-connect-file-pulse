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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class XMLCommonConfig extends AbstractConfig {

    public static final String XML_FORCE_ARRAY_ON_FIELDS_CONFIG = "xml.force.array.on.fields";
    private static final String XML_FORCE_ARRAY_ON_FIELDS_FORCE_DOC = "The comma-separated list of fields for which an array-type must be forced";

    public static final String XML_PARSER_VALIDATING_ENABLED_CONFIG = "xml.parser.validating.enabled";
    private static final String XML_PARSER_VALIDATING_ENABLED_DOC = " Specifies that the parser will validate documents as they are parsed (default: false).";

    public static final String XML_PARSER_NAMESPACE_AWARE_ENABLED_CONFIG = "xml.parser.namespace.aware.enabled";
    private static final String XML_PARSER_NAMESPACE_AWARE_ENABLED_DOC = "Specifies that the XML parser will provide support for XML namespaces (default: false).";

    public static final String XML_EXCLUDE_EMPTY_ELEMENTS_CONFIG = "xml.exclude.empty.elements";
    private static final String XML_EXCLUDE_EMPTY_ELEMENTS_DOC = "Specifies that the reader should exclude element having no field (default: false).";

    public static final String XML_EXCLUDE_NODE_ATTRIBUTES_CONFIG = "xml.exclude.node.attributes";
    private static final String XML_EXCLUDE_NODE_ATTRIBUTES_DOC = "Specifies that the reader should exclude all node attributes (default: false).";

    public static final String XML_EXCLUDE_NODE_ATTRIBUTES_IN_NAMESPACES_CONFIG = "xml.exclude.node.attributes.in.namespaces";
    private static final String XML_EXCLUDE_NODE_ATTRIBUTES_IN_NAMESPACES_DOC = "Specifies that the reader should only exclude node attributes in the defined list of namespaces.";

    public static final String XML_DATA_TYPE_INFERENCE_ENABLED_CONFIG = "xml.data.type.inference.enabled";
    private static final String XML_DATA_TYPE_INFERENCE_ENABLED_DOC = "Specifies that the reader should try to infer the type of data nodes (default: false).";

    public static final String XML_ATTRIBUTE_PREFIX_CONFIG = "xml.attribute.prefix";
    private static final String XML_ATTRIBUTE_PREFIX_DOC = "If set, the name of attributes will be prepended with the specified prefix when they are added to a record (default: '').";

    public static final String XML_TEXT_NODE_VALUE_FIELD_NAME_CONFIG = "xml.text.node.value.field.name";
    private static final String XML_TEXT_NODE_VALUE_FIELD_NAME_DEFAULT = "value";
    private static final String XML_TEXT_NODE_VALUE_FIELD_NAME_DOC = "Specifies the name to be used for naming the field that will contain the value of a TextNode element having attributes. (default: 'value').";

    private final String keyPrefix;

    /**
     * Creates a new {@link XMLCommonConfig} instance.
     *
     * @param originals the reader configuration.
     */
    protected XMLCommonConfig(final String keyPrefix,
                              final ConfigDef configDef,
                              final Map<String, ?> originals) {
        super(configDef, originals);
        this.keyPrefix = keyPrefix;
    }

    private String withKeyPrefix(final String configKey) {
        return keyPrefix + configKey;
    }

    public boolean isValidatingEnabled() {
        return getBoolean(withKeyPrefix(XML_PARSER_VALIDATING_ENABLED_CONFIG));
    }

    public boolean isNamespaceAwareEnabled() {
        return getBoolean(withKeyPrefix(XML_PARSER_NAMESPACE_AWARE_ENABLED_CONFIG));
    }

    public boolean isEmptyElementExcluded() {
        return getBoolean(withKeyPrefix(XML_EXCLUDE_EMPTY_ELEMENTS_CONFIG));
    }

    public boolean isNodeAttributesExcluded() {
        return getBoolean(withKeyPrefix(XML_EXCLUDE_NODE_ATTRIBUTES_CONFIG));
    }

    public Set<String> getExcludeNodeAttributesInNamespaces() {
        return new HashSet<>(getList(withKeyPrefix(XML_EXCLUDE_NODE_ATTRIBUTES_IN_NAMESPACES_CONFIG)));
    }

    public String getAttributePrefix() {
        return getString(withKeyPrefix(XML_ATTRIBUTE_PREFIX_CONFIG));
    }

    public String getTextNodeValueFieldName() {
        return getString(withKeyPrefix(XML_TEXT_NODE_VALUE_FIELD_NAME_CONFIG));
    }

    public boolean isDataTypeInferenceEnabled() {
        return getBoolean(withKeyPrefix(XML_DATA_TYPE_INFERENCE_ENABLED_CONFIG));
    }

    public List<String> forceArrayFields() {
        return getList(withKeyPrefix(XML_FORCE_ARRAY_ON_FIELDS_CONFIG));
    }

    public static ConfigDef buildConfigDefWith(final String group,
                                               final String keyPrefix,
                                               final ConfigDef.ConfigKey... additional) {
        return buildConfigDefWith(group, keyPrefix, Arrays.asList(additional));
    }
    public static ConfigDef buildConfigDefWith(final String group,
                                               final String keyPrefix,
                                               final Iterable<ConfigDef.ConfigKey> additional) {
        int filterGroupCounter = 0;
        final ConfigDef def = new ConfigDef()
                .define(
                        keyPrefix + XML_FORCE_ARRAY_ON_FIELDS_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.MEDIUM,
                        XML_FORCE_ARRAY_ON_FIELDS_FORCE_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        XML_FORCE_ARRAY_ON_FIELDS_FORCE_DOC
                )
                .define(
                        keyPrefix + XML_PARSER_VALIDATING_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        XML_PARSER_VALIDATING_ENABLED_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_PARSER_VALIDATING_ENABLED_CONFIG
                )
                .define(
                        keyPrefix + XML_PARSER_NAMESPACE_AWARE_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        XML_PARSER_NAMESPACE_AWARE_ENABLED_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_PARSER_NAMESPACE_AWARE_ENABLED_CONFIG
                )
                .define(
                        keyPrefix + XML_EXCLUDE_EMPTY_ELEMENTS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        XML_EXCLUDE_EMPTY_ELEMENTS_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_EXCLUDE_EMPTY_ELEMENTS_CONFIG
                )
                .define(
                        keyPrefix + XML_EXCLUDE_NODE_ATTRIBUTES_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        XML_EXCLUDE_NODE_ATTRIBUTES_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_EXCLUDE_NODE_ATTRIBUTES_CONFIG
                )
                .define(
                        keyPrefix + XML_EXCLUDE_NODE_ATTRIBUTES_IN_NAMESPACES_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.LOW,
                        XML_EXCLUDE_NODE_ATTRIBUTES_IN_NAMESPACES_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_EXCLUDE_NODE_ATTRIBUTES_IN_NAMESPACES_CONFIG
                )
                .define(
                        keyPrefix + XML_DATA_TYPE_INFERENCE_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        XML_DATA_TYPE_INFERENCE_ENABLED_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_EXCLUDE_EMPTY_ELEMENTS_CONFIG
                )
                .define(
                        keyPrefix + XML_ATTRIBUTE_PREFIX_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        XML_ATTRIBUTE_PREFIX_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_ATTRIBUTE_PREFIX_CONFIG
                )
                .define(
                        keyPrefix + XML_TEXT_NODE_VALUE_FIELD_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        XML_TEXT_NODE_VALUE_FIELD_NAME_DEFAULT,
                        ConfigDef.Importance.LOW,
                        XML_TEXT_NODE_VALUE_FIELD_NAME_DOC,
                        group,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        keyPrefix + XML_TEXT_NODE_VALUE_FIELD_NAME_CONFIG
                );

        for (ConfigDef.ConfigKey configKey : additional) {
            def.define(
                configKey.name,
                configKey.type,
                configKey.defaultValue,
                configKey.validator,
                configKey.importance,
                configKey.documentation,
                group,
                filterGroupCounter++,
                configKey.width,
                configKey.displayName
            );
        }
        return def;
    }
}
