/*
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

public class FailFilterConfig extends CommonFilterConfig {

    public static final String MESSAGE_CONFIG = "message";
    public static final String MESSAGE_DOC = "The error message thrown by the filter.";

    /**
     * Creates a new {@link FailFilterConfig} instance.
     * @param originals the configuration.
     */
    public FailFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public String message() {
        return getString(MESSAGE_CONFIG);
    }

    public static ConfigDef configDef() {
        return CommonFilterConfig.configDef()
                .define(MESSAGE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, MESSAGE_DOC);
    }
}