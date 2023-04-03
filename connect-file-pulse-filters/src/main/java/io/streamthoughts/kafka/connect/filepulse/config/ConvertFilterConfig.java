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

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class ConvertFilterConfig extends CommonFilterConfig {

    private static final String GROUP_FILTER_CONVERT = "CONVERT_FILTER";

    public static final String CONVERT_FIELD_CONFIG = "field";
    private static final String CONVERT_FIELD_DOC = "The field to convert (dot notation is supported)";

    public static final String CONVERT_TO_CONFIG = "to";
    private static final String CONVERT_TO_DOC = "The type to which the field must be converted";

    public static final String CONVERT_DEFAULT_CONFIG = "default";
    private static final String CONVERT_DEFAULT_DOC = "The default value to apply if the field cannot be converted";

    public static final String CONVERT_IGNORE_MISSING_CONFIG = "ignoreMissing";
    private static final String CONVERT_IGNORE_MISSING_DOC = "If true and field does not exist the filter will be apply successfully without modifying the value. If field is null the schema will be modified.";

    /**
     * Creates a new {@link ConvertFilterConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public ConvertFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String field() {
        return getString(CONVERT_FIELD_CONFIG);
    }

    public Type to() {
        return Type.valueOf(getString(CONVERT_TO_CONFIG).toUpperCase());
    }

    public TypedValue defaultValue() {
        String defaultValue = getString(CONVERT_DEFAULT_CONFIG);
        return defaultValue != null ? TypedValue.any(defaultValue).as(to()) : null;
    }

    public boolean ignoreMissing() {
        return getBoolean(CONVERT_IGNORE_MISSING_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        CONVERT_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CONVERT_FIELD_DOC,
                        GROUP_FILTER_CONVERT,
                        filterGroupCounter,
                        ConfigDef.Width.NONE,
                        CONVERT_FIELD_CONFIG
                )

                .define(
                        CONVERT_TO_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CONVERT_TO_DOC,
                        GROUP_FILTER_CONVERT,
                        filterGroupCounter,
                        ConfigDef.Width.NONE,
                        CONVERT_TO_CONFIG
                )

                .define(
                        CONVERT_DEFAULT_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        CONVERT_DEFAULT_DOC,
                        GROUP_FILTER_CONVERT,
                        filterGroupCounter,
                        ConfigDef.Width.NONE,
                        CONVERT_DEFAULT_CONFIG
                )

                .define(
                        CONVERT_IGNORE_MISSING_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.HIGH,
                        CONVERT_IGNORE_MISSING_DOC,
                        GROUP_FILTER_CONVERT,
                        filterGroupCounter,
                        ConfigDef.Width.NONE,
                        CONVERT_IGNORE_MISSING_CONFIG
                );
    }
}