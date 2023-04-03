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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class JSONFilterConfig extends CommonFilterConfig {

    private static final String JSON_FILTER = "JSON_FILTER";

    public static final String JSON_TARGET_CONFIG = "target";
    public static final String JSON_TARGET_DOC = "The target field to put the parsed JSON value (optional)";

    public static final String JSON_MERGE_CONFIG = "merge";
    public static final String JSON_MERGE_DOC = "A boolean that specifies whether to merge the JSON " +
            "object into the top level of the input record (default: false).";

    public static final String JSON_EXPLODE_ARRAY_CONFIG = "explode.array";
    public static final String JSON_EXPLODE_ARRAY_DOC = "A boolean that specifies whether to explode arrays " +
            "                                       into separate records (default: false)";

    public static final String JSON_SOURCE_CHARSET_CONFIG = "source.charset";
    public static final String JSON_SOURCE_CHARSET_DOC = "The charset to be used for reading the source " +
            "                                       field (default: UTF-8)";

    /**
     * Creates a new {@link JSONFilterConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public JSONFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String source() {
        return getString(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG);
    }

    public String target() {
        return getString(JSON_TARGET_CONFIG);
    }

    public boolean explode() {
        return getBoolean(JSON_EXPLODE_ARRAY_CONFIG);
    }

    public boolean merge() {
        return getBoolean(JSON_MERGE_CONFIG);
    }

    public Charset charset() {
        String name = getString(JSON_SOURCE_CHARSET_CONFIG);
        return name == null ? StandardCharsets.UTF_8 : Charset.forName(name);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(getOverwriteConfigKey(JSON_FILTER, filterGroupCounter++))
                .define(getSourceConfigKey(JSON_FILTER, filterGroupCounter++))
                .define(
                        JSON_TARGET_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        JSON_TARGET_DOC,
                        JSON_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        JSON_TARGET_CONFIG
                )
                .define(
                        JSON_SOURCE_CHARSET_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        JSON_SOURCE_CHARSET_DOC,
                        JSON_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        JSON_SOURCE_CHARSET_CONFIG
                )
                .define(
                        JSON_EXPLODE_ARRAY_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.MEDIUM,
                        JSON_EXPLODE_ARRAY_DOC,
                        JSON_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        JSON_EXPLODE_ARRAY_CONFIG
                )
                .define(
                        JSON_MERGE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.MEDIUM,
                        JSON_MERGE_DOC,
                        JSON_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        JSON_MERGE_CONFIG
                );
    }
}