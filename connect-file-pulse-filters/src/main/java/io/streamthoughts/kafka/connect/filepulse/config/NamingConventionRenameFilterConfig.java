/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import static io.streamthoughts.kafka.connect.filepulse.config.NamingConvention.CAMEL_CASE;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


public class NamingConventionRenameFilterConfig extends AbstractConfig {
    public static final String FIELD_NAMING_CONVENTION_CONFIG = "naming.convention";
    private static final String FIELD_NAMING_CONVENTION_DOC = "Default field naming convention, possible values are: camelCase, snakeCase, pascalCase";
    public static final String FIELD_NAMING_CONVENTION_DELIMITER_CONFIG = "delimiter";
    private static final String FIELD_NAMING_CONVENTION_DELIMITER_DOC_TEMPLATE = "Set of characters to determine casing of the field, default values are %s";
    private static final String FIELD_NAMING_CONVENTION_DELIMITER_DEFAULT = "_ ,-()[]{}";

    public NamingConventionRenameFilterConfig(Map<?, ?> originals) {
        super(getConfigDef(), originals);
    }

    public String getDefaultNamingConvention() {
        return getString(FIELD_NAMING_CONVENTION_CONFIG);
    }

    public char[] getColumnHeaderDelimiters() {
        return getString(FIELD_NAMING_CONVENTION_DELIMITER_CONFIG).toCharArray();
    }

    public static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(FIELD_NAMING_CONVENTION_CONFIG,
                        ConfigDef.Type.STRING,
                        CAMEL_CASE.getConfigValue(),
                        ConfigDef.Importance.HIGH,
                        FIELD_NAMING_CONVENTION_DOC)
                .define(FIELD_NAMING_CONVENTION_DELIMITER_CONFIG,
                        ConfigDef.Type.STRING,
                        FIELD_NAMING_CONVENTION_DELIMITER_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        String.format(FIELD_NAMING_CONVENTION_DELIMITER_DOC_TEMPLATE, FIELD_NAMING_CONVENTION_DELIMITER_DEFAULT));
    }
}
