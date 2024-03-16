/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class SplitFilterConfig extends CommonFilterConfig {

    private static final String GROUP_SPLIT_FILTER = "SPLIT_FILTER";

    public static final String MUTATE_SPLIT_CONFIG = "split";
    private static final String MUTATE_SPLIT_DOC = "The comma-separated list of fields to split";

    public static final String MUTATE_SPLIT_SEP_CONFIG = "separator";
    private static final String MUTATE_SPLIT_SEP_DOC = "The separator used for splitting a message field's value to array (default separator : ',')";


    private static final List<Object> DEFAULT_VALUE = Collections.emptyList();

    /**
     * Creates a new {@link SplitFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public SplitFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }


    public List<String> split() {
        return getList(MUTATE_SPLIT_CONFIG);
    }

    public String splitSeparator() {
        return getString(MUTATE_SPLIT_SEP_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        MUTATE_SPLIT_SEP_CONFIG,
                        ConfigDef.Type.STRING, ",",
                        ConfigDef.Importance.HIGH,
                        MUTATE_SPLIT_SEP_DOC,
                        GROUP_SPLIT_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MUTATE_SPLIT_SEP_CONFIG
                )
                .define(
                        MUTATE_SPLIT_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        MUTATE_SPLIT_DOC,
                        GROUP_SPLIT_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MUTATE_SPLIT_CONFIG
                );
    }
}
