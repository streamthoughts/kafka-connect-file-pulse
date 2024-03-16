/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class RenameFilterConfig extends CommonFilterConfig {

    private static final String GROUP_RENAME_FILTER = "RENAME_FILTER";

    public static final String RENAME_FIELD_CONFIG = "field";
    private static final String RENAME_FIELD_DOC = "The field to rename";

    public static final String RENAME_TARGET_CONFIG = "target";
    private static final String RENAME_TARGET_DOC = "The target name";

    public static final String RENAME_IGNORE_MISSING_CONFIG = "ignoreMissing";
    private static final String RENAME_IGNORE_MISSING_DOC = "If true and field does not exist the filter will be apply successfully without modifying the value. If field is null the schema will be modified.";

    /**
     * Creates a new {@link RenameFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public RenameFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }


    public String field() {
        return getString(RENAME_FIELD_CONFIG);
    }

    public String target() {
        return getString(RENAME_TARGET_CONFIG);
    }

    public boolean ignoreMissing() {
        return getBoolean(RENAME_IGNORE_MISSING_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        RENAME_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        RENAME_FIELD_DOC,
                        GROUP_RENAME_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        RENAME_FIELD_CONFIG
                )

                .define(
                        RENAME_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        RENAME_TARGET_DOC,
                        GROUP_RENAME_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        RENAME_TARGET_CONFIG
                )

                .define(
                        RENAME_IGNORE_MISSING_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.HIGH,
                        RENAME_IGNORE_MISSING_DOC,
                        GROUP_RENAME_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        RENAME_IGNORE_MISSING_CONFIG
                );

    }
}
