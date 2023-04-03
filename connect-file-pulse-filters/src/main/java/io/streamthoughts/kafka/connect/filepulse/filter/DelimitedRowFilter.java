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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.ConfigUtils;

public class DelimitedRowFilter extends AbstractDelimitedRowFilter<DelimitedRowFilter> {

    public static final String READER_FIELD_SEPARATOR_CONFIG = "regex";
    public static final String READER_FIELD_SEPARATOR_CONFIG_ALIAS = "separator";
    public static final String READER_FIELD_SEPARATOR_DEFAULT = ";";
    private static final String READER_FIELD_SEPARATOR_DOC =
            "The character delimiter or regex used to split each column value (default: ';').";
    private static final String CONFIG_GROUP = "Delimited Row Filter";

    private Pattern pattern = null;

    private String delimiter;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(ConfigUtils.translateDeprecatedConfigs(configs, new String[][]{
                {READER_FIELD_SEPARATOR_CONFIG, READER_FIELD_SEPARATOR_CONFIG_ALIAS}
        }));

        delimiter = filterConfig().getString(READER_FIELD_SEPARATOR_CONFIG);

        if (!StringUtils.isFastSplit(delimiter)) {
            pattern = Pattern.compile(delimiter);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String[] parseColumnsValues(final String line) {
        // set limit to -1 so that trailing empty strings will NOT be discarded.
        return pattern != null ? pattern.split(line, -1) : line.split(delimiter, -1);
    }

    @Override
    public ConfigDef configDef() {
        int filterGroupCounter = 0;
        return super.configDef()
                .define(
                        READER_FIELD_SEPARATOR_CONFIG,
                        ConfigDef.Type.STRING,
                        READER_FIELD_SEPARATOR_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        READER_FIELD_SEPARATOR_DOC,
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        READER_FIELD_SEPARATOR_CONFIG
                )
                .define(
                        READER_FIELD_SEPARATOR_CONFIG_ALIAS,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        "Deprecated. Use " + READER_FIELD_SEPARATOR_CONFIG + " instead.",
                        CONFIG_GROUP,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        READER_FIELD_SEPARATOR_CONFIG_ALIAS
                );
    }
}