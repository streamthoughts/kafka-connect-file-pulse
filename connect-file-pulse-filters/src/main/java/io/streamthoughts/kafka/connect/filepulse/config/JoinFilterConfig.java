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

public class JoinFilterConfig extends CommonFilterConfig {

    private static final String GROUP_JOIN_FILTER = "JOIN_FILTER";

    public static final String JOIN_FIELD_CONFIG = "field";
    private static final String JOIN_FIELD_DOC = "The field get the array from.";

    public static final String JOIN_TARGET_CONFIG = "target";
    private static final String JOIN_TARGET_DOC = "The target field to assign the joined value " +
            "(by default the field value is used).";

    public static final String JOIN_SEPARATOR_CONFIG = "separator";
    private static final String JOIN_SEPARATOR_DOC = "The separator used for joining array values (default=',')";

    /**
     * Creates a new {@link JoinFilterConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public JoinFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String field() {
        return getString(JOIN_FIELD_CONFIG);
    }

    public String target() {
        return getString(JOIN_TARGET_CONFIG);
    }

    public String separator() {
        return getString(JOIN_SEPARATOR_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        JOIN_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        JOIN_FIELD_DOC,
                        GROUP_JOIN_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        JOIN_FIELD_CONFIG
                )
                .define(
                        JOIN_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        JOIN_TARGET_DOC,
                        GROUP_JOIN_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        JOIN_TARGET_CONFIG
                )
                .define(
                        JOIN_SEPARATOR_CONFIG,
                        ConfigDef.Type.STRING, ",",
                        ConfigDef.Importance.MEDIUM,
                        JOIN_SEPARATOR_DOC,
                        GROUP_JOIN_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        JOIN_SEPARATOR_CONFIG
                );
    }
}
