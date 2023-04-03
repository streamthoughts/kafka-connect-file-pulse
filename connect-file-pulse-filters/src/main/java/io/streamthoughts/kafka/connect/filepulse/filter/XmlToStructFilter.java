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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.FieldPaths;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.xml.XMLCommonConfig;
import io.streamthoughts.kafka.connect.filepulse.xml.XMLDocumentReader;
import io.streamthoughts.kafka.connect.filepulse.xml.XMLNodeToStructConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * This {@link RecordFilter} can be used to parse a field containing a XML Document into struct record.
 */
public class XmlToStructFilter extends AbstractRecordFilter<XmlToStructFilter> {

    private static final Logger LOG = LoggerFactory.getLogger(XmlToStructFilter.class);

    private XMLDocumentReader reader;

    private XMLNodeToStructConverter converter;

    private String source;


    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return XmlToStructFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);

        var filterConfig = new XmlToStructFilterConfig(configs);
        this.converter = new XMLNodeToStructConverter()
                .setExcludeEmptyElement(filterConfig.isEmptyElementExcluded())
                .setExcludeAllAttributes(filterConfig.isNodeAttributesExcluded())
                .setExcludeAttributesInNamespaces(filterConfig.getExcludeNodeAttributesInNamespaces())
                .setForceArrayFields(FieldPaths.from(filterConfig.forceArrayFields()))
                .setForceContentFields(FieldPaths.from(filterConfig.getForceContentFields()))
                .setTypeInferenceEnabled(filterConfig.isDataTypeInferenceEnabled())
                .setContentFieldName(filterConfig.getContentFieldName())
                .setFieldCharactersRegexPattern(filterConfig.getXmlFieldCharactersRegexPattern())
                .setFieldCharactersStringReplacement(filterConfig.getXmlFieldCharactersStringReplacement())
                .setAttributePrefix(filterConfig.getAttributePrefix());

        this.reader = new XMLDocumentReader(
                filterConfig.isNamespaceAwareEnabled(),
                filterConfig.isValidatingEnabled()
        );

        this.source = filterConfig.getSource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {
        final TypedValue value = checkIsNotNull(record.get(source));
        switch (value.type()) {
            case STRING:
                final String xml = value.getString();
                if (StringUtils.isBlank(value.getString())) {
                    return RecordsIterable.empty();
                }
                return parseDocument(xml.getBytes(StandardCharsets.UTF_8), context);
            case BYTES:
                byte[] bytes = value.getBytes();
                if (bytes.length == 0) {
                    return RecordsIterable.empty();
                }
                return parseDocument(bytes, context);
            default:
                throw new FilterException(
                        "Invalid field '" + source + "' was passed through the " +
                                "connector's configuration'. " +
                                "Cannot parse field of type '" + value.type() + "' to XML."
                );
        }
    }

    public RecordsIterable<TypedStruct> parseDocument(final byte[] bytes, final FilterContext context) {
        try {
            final Document document = reader.parse(new ByteArrayInputStream(bytes), new ErrorHandler() {
                @Override
                public void warning(final SAXParseException e) {
                    LOG.warn(
                            "Handled XML parser warning on file {}. Error: {}",
                            context.metadata().uri(),
                            e.getLocalizedMessage()
                    );
                }

                @Override
                public void error(final SAXParseException e) {
                    LOG.warn(
                            "Handled XML parser error on file {}. Error: {}",
                            context.metadata().uri(),
                            e.getLocalizedMessage()
                    );
                }

                @Override
                public void fatalError(final SAXParseException e) {
                    throw new ReaderException(
                            "Handled XML parser fatal error on file '" + context.metadata().uri() + "'",
                            e
                    );
                }
            });
            return RecordsIterable.of(converter.apply(document));
        } catch (IOException | SAXException e) {
            throw new FilterException("Failed to parse and convert XML document into STRUCT object", e);
        }
    }

    private TypedValue checkIsNotNull(final TypedValue value) {
        if (value.isNull()) {
            throw new FilterException(
                    "Invalid field '" + source + "' was passed through the " +
                            "connector's configuration'. Cannot parse null or empty value to XML."
            );
        }
        return value;
    }

    static class XmlToStructFilterConfig extends XMLCommonConfig {

        private static final String GROUP = "XmlToStruct";

        /**
         * Creates a new {@link XMLCommonConfig} instance.
         *
         * @param originals the reader configuration.
         */
        protected XmlToStructFilterConfig(final Map<String, ?> originals) {
            super("", configDef(), originals);
        }

        public static ConfigDef configDef() {
            final ConfigDef def = new ConfigDef(CommonFilterConfig.configDef());
            final ConfigDef xmlConfigDef = XMLCommonConfig.buildConfigDefWith(
                    GROUP,
                    "",
                    CommonFilterConfig.getSourceConfigKey()
            );
            xmlConfigDef.configKeys().values().forEach(def::define);
            return def;
        }

        public String getSource() {
            return getString(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG);
        }
    }
}
