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

import io.streamthoughts.kafka.connect.filepulse.filter.config.CommonFilterConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ExplodeFilterConfig extends CommonFilterConfig {

    /**
     * Creates a new {@link ExplodeFilterConfig} instance.
     *
     * @param originals the filter configuration.
     */
    public ExplodeFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String source() {
        return getString(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG);
    }

    public static ConfigDef configDef() {
        ConfigDef def = CommonFilterConfig.configDef();
        CommonFilterConfig.withOverwrite(def);
        CommonFilterConfig.withSource(def);
        return def;
    }
}
