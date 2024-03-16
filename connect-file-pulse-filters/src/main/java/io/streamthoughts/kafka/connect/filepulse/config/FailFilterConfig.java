/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class FailFilterConfig extends CommonFilterConfig {

    private static final String GROUP_FAIL_FILTER = "FAIL_FILTER";

    public static final String MESSAGE_CONFIG = "message";
    public static final String MESSAGE_DOC = "The error message thrown by the filter.";

    /**
     * Creates a new {@link FailFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public FailFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String message() {
        return getString(MESSAGE_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        MESSAGE_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        MESSAGE_DOC,
                        GROUP_FAIL_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MESSAGE_CONFIG
                );
    }
}