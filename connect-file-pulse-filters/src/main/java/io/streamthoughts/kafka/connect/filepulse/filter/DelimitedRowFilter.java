/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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