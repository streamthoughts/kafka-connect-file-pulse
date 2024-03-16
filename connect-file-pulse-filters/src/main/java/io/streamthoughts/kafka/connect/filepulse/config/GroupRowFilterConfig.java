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

public class GroupRowFilterConfig extends CommonFilterConfig {

    private static final String GROUP_FILTER_GROUP_ROW = "GROUP_ROW_FILTER";

    public static final String FIELDS_CONFIG = "fields";
    private static final String FIELDS_DOC = "List of fields used to regroup records";

    public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
    private static final String MAX_BUFFERED_RECORDS_DOC = "The maximum number of records to group (default : -1).";

    public static final String TARGET_CONFIG = "target";
    private static final String TARGET_DOC = "The target array field to put the grouped field (default : records).";

    /**
     * Creates a new {@link GroupRowFilterConfig} instance.
     *
     * @param originals the originals configs.
     */
    public GroupRowFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String target() {
        return getString(TARGET_CONFIG);
    }

    public List<String> fields() {
        return getList(FIELDS_CONFIG);
    }

    public int maxBufferedRecords() {
        return getInt(MAX_BUFFERED_RECORDS_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        FIELDS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        FIELDS_DOC,
                        GROUP_FILTER_GROUP_ROW,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        FIELDS_CONFIG
                )
                .define(
                        MAX_BUFFERED_RECORDS_CONFIG,
                        ConfigDef.Type.INT,
                        -1,
                        ConfigDef.Importance.HIGH,
                        MAX_BUFFERED_RECORDS_DOC,
                        GROUP_FILTER_GROUP_ROW,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MAX_BUFFERED_RECORDS_CONFIG
                )
                .define(
                        TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        "records",
                        ConfigDef.Importance.HIGH,
                        TARGET_DOC,
                        GROUP_FILTER_GROUP_ROW,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        TARGET_CONFIG
                );

    }
}
