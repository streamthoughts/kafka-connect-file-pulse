/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class AppendFilterConfig extends CommonFilterConfig {

    private static final String GROUP_APPEND = "APPEND_FILTER";

    public static final String APPEND_FIELD_CONFIG = "field";
    public static final String APPEND_FIELD_DOC = "The field to append";

    public static final String APPEND_VALUE_CONFIG = "value";
    public static final String APPEND_VALUE_DOC = "The value to be appended";

    public static final String APPEND_OVERWRITE_CONFIG = "overwrite";
    public static final String APPEND_OVERWRITE_DOC = "overwrite existing field";

    /**
     * Creates a new {@link CommonFilterConfig} instance.
     *
     * @param originals the origina
     */
    public AppendFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public Expression field() {
        return ExpressionParsers.parseExpression(getString(APPEND_FIELD_CONFIG));
    }

    public boolean isOverwritten() {
        return getBoolean(APPEND_OVERWRITE_CONFIG);
    }

    public String value() {
        return getString(APPEND_VALUE_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        APPEND_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        APPEND_FIELD_DOC,
                        GROUP_APPEND,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        APPEND_FIELD_CONFIG
                )

                .define(
                        APPEND_OVERWRITE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        APPEND_OVERWRITE_DOC,
                        GROUP_APPEND,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        APPEND_OVERWRITE_CONFIG
                )

                .define(
                        APPEND_VALUE_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        APPEND_VALUE_DOC,
                        GROUP_APPEND,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        APPEND_VALUE_CONFIG
                );
    }
}
