/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class MoveFilterConfig extends CommonFilterConfig {

    private static final String GROUP_MOVE = "MOVE_FILTER";

    public static final String MOVE_TARGET_CONFIG = "target";
    private static final String MOVE_TARGET_DOC = "The target path (support dot-notation)";

    public static final String MOVE_IGNORE_MISSING_CONFIG = "ignoreMissing";
    private static final String MOVE_IGNORE_MISSING_DOC = "If true and field does not exist the filter will be " +
            "apply successfully without modifying the value. " +
            "If field is null the  will be modified.";

    /**
     * Creates a new {@link MoveFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public MoveFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String target() {
        return getString(MOVE_TARGET_CONFIG);
    }

    public boolean ignoreMissing() {
        return getBoolean(MOVE_IGNORE_MISSING_CONFIG);
    }

    public String source() {
        return this.getString(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(getOverwriteConfigKey(GROUP_MOVE, filterGroupCounter++))
                .define(getSourceConfigKey(GROUP_MOVE, filterGroupCounter++))
                .define(
                        MOVE_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        MOVE_TARGET_DOC
                )
                .define(
                        MOVE_IGNORE_MISSING_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.HIGH,
                        MOVE_IGNORE_MISSING_DOC
                );
    }
}
