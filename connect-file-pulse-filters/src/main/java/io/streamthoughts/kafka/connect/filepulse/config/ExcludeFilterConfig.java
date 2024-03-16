/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class ExcludeFilterConfig extends CommonFilterConfig {

    private static final String GROUP_EXCLUDE_FILTER = "EXCLUDE_FILTER";

    public static final String EXCLUDE_FIELDS_CONFIG = "fields";
    private static final String EXCLUDE_FIELDS_DOC = "The comma-separated list of field names to exclude";

    /**
     * Creates a new {@link RenameFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public ExcludeFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }


    public List<String> fields() {
        return getList(EXCLUDE_FIELDS_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        EXCLUDE_FIELDS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        EXCLUDE_FIELDS_DOC,
                        GROUP_EXCLUDE_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        EXCLUDE_FIELDS_CONFIG
                );

    }
}
