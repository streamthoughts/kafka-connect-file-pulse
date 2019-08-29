/*
 * Copyright 2019 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.filter.config.CommonFilterConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JSONFilterConfig extends CommonFilterConfig {

    public static final String JSON_TARGET_CONFIG    = "target";
    public static final String JSON_TARGET_DOC       = "The target field to put the parsed JSON value";

    /**
     * Creates a new {@link JSONFilterConfig} instance.
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

    public Set<String> overwrite() {
        return new HashSet<>(getList(CommonFilterConfig.FILTER_OVERWRITE_CONFIG));
    }

    public static ConfigDef configDef() {
        ConfigDef def = CommonFilterConfig.configDef()
                .define(JSON_TARGET_CONFIG, ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.HIGH, JSON_TARGET_DOC);
        CommonFilterConfig.withOverwrite(def);
        CommonFilterConfig.withSource(def);

        return def;
    }
}