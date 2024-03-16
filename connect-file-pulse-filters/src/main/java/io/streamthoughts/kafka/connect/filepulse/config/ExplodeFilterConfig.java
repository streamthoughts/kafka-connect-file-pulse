/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class ExplodeFilterConfig extends CommonFilterConfig {

    private static final String FILTER_EXPLODE = "EXPLODE_FILTER";

    /**
     * Creates a new {@link ExplodeFilterConfig} instance.
     *
     * @param originals the filter configuration.
     */
    public ExplodeFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(getOverwriteConfigKey(FILTER_EXPLODE, filterGroupCounter++))
                .define(getSourceConfigKey(FILTER_EXPLODE, filterGroupCounter++));
    }
}
