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

public class ExcludeFieldsMatchingPatternConfig extends CommonFilterConfig {
    public static final String EXCLUDE_FIELDS_REGEX_CONFIG = "regex";

    private static final String EXCLUDE_FIELDS_REGEX_CONFIG_DOC = "Regexp pattern applied to a field value to determine if the fields should be propagated or not.";

    public static final String EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG = "block.field";

    private static final String EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG_DOC = "If true, omits propagating the field downstream. Otherwise, propagates the field with a null value";

    public ExcludeFieldsMatchingPatternConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(EXCLUDE_FIELDS_REGEX_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        EXCLUDE_FIELDS_REGEX_CONFIG_DOC)
                .define(EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG_DOC);
    }

    public Pattern pattern() {
        return Pattern.compile(this.getString(EXCLUDE_FIELDS_REGEX_CONFIG));
    }

    public boolean blockField() {
        return this.getBoolean(EXCLUDE_FIELDS_BLOCK_FIELD_CONFIG);
    }

}
