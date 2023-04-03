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
package io.streamthoughts.kafka.connect.filepulse.config;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigDef;

public class XmlToJsonFilterConfig extends CommonFilterConfig {

    private static final String FILTER = "XmlToJsonFilter";

    public static final String XML_PARSER_KEEP_STRINGS_CONFIG = "xml.parser.keep.strings";
    public static final String XML_PARSER_KEEP_STRINGS_DOC = "When parsing the XML into JSON, specifies if values should be kept " +
                                                         "as strings (true), or if " +
                                                         "they should try to be guessed into JSON values (numeric, boolean, string)";

    public static final String XML_PARSER_CDATA_TAG_NAME_DEFAULT = "value";
    public static final String XML_PARSER_CDATA_TAG_NAME_CONFIG = "xml.parser.cDataTagName";
    public static final String XML_PARSER_CDATA_TAG_NAME_DOC = "The name of the key in a JSON Object that indicates " +
                                                               "a CDATA section (default: '" + XML_PARSER_CDATA_TAG_NAME_DEFAULT + "').";

    public static final String XML_PARSER_SOURCE_CHARSET_CONFIG = "source.charset";
    public static final String XML_PARSER_SOURCE_CHARSET_DOC = "The charset to be used for reading the source " +
            "                                                   field (default: UTF-8)";

    /**
     * Creates a new {@link XmlToJsonFilterConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public XmlToJsonFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(getOverwriteConfigKey(FILTER, filterGroupCounter++))
                .define(getSourceConfigKey(FILTER, filterGroupCounter++))
                .define(
                        XML_PARSER_KEEP_STRINGS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        XML_PARSER_KEEP_STRINGS_DOC,
                        FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        XML_PARSER_KEEP_STRINGS_CONFIG
                )
                .define(
                        XML_PARSER_CDATA_TAG_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        XML_PARSER_CDATA_TAG_NAME_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        XML_PARSER_CDATA_TAG_NAME_DOC,
                        FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        XML_PARSER_CDATA_TAG_NAME_CONFIG
                )
                .define(
                        XML_PARSER_SOURCE_CHARSET_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        XML_PARSER_SOURCE_CHARSET_DOC,
                        FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        XML_PARSER_SOURCE_CHARSET_CONFIG
                );
    }

    public boolean getXmlParserKeepStrings() {
        return getBoolean(XML_PARSER_KEEP_STRINGS_CONFIG);
    }

    public String getCDataTagName() {
        return getString(XML_PARSER_CDATA_TAG_NAME_CONFIG);
    }

    public Charset charset() {
        return Optional.ofNullable(getString(XML_PARSER_SOURCE_CHARSET_CONFIG))
                .map(Charset::forName)
                .orElse(StandardCharsets.UTF_8);
    }
}
