/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;

public class ExtractValueConfig extends CommonFilterConfig {
    public static final String REGEX_FIELD_CONFIG = "field";

    private static final String REGEX_FIELD_CONFIG_DOC = "The field to extract the data from.";

    public static final String EXTRACT_TARGET_CONFIG = "target";

    private static final String EXTRACT_TARGET_DOC = "(Optional) The target field. If not defined, source field will be overwritten";

    public static final String REGEX_CONFIG = "regex";

    private static final String REGEX_CONFIG_DOC = "Regexp pattern applied to a field value to extract the desired value out of a specific field.";

    public static final String REGEX_DEFAULT_VALUE_CONFIG = "default.value";

    private static final String REGEX_DEFAULT_VALUE_CONFIG_DOC = "Default value applied when regex returns nothing.";


    public ExtractValueConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(REGEX_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        REGEX_FIELD_CONFIG_DOC)
                .define(REGEX_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        REGEX_CONFIG_DOC)
                .define(REGEX_DEFAULT_VALUE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        REGEX_DEFAULT_VALUE_CONFIG_DOC)
                .define(EXTRACT_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        EXTRACT_TARGET_DOC);
    }

    public Pattern pattern() {
        return Pattern.compile(getString(REGEX_CONFIG));
    }

    public String getFieldName() {
        return getString(REGEX_FIELD_CONFIG);
    }

    public String getDefaultValue() {
        return getString(REGEX_DEFAULT_VALUE_CONFIG);
    }

    public String getTargetName() {
        return getString(EXTRACT_TARGET_CONFIG);
    }

}
