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

import io.streamthoughts.kafka.connect.transform.GrokConfig;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class GrokFilterConfig extends CommonFilterConfig {

    private final GrokConfig grok;
    public static final String GROK_FILTER = "GROK_FILTER";

    /**
     * Creates a new {@link GrokFilterConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public GrokFilterConfig(final Map<String, ?> originals) {
        super(configDef() , originals);
        grok = new GrokConfig(originals);
    }

    public GrokConfig grok() {
        return grok;
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        final ConfigDef def = new ConfigDef(CommonFilterConfig.configDef())
                .define(getSourceConfigKey(GROK_FILTER, filterGroupCounter++))
                .define(getOverwriteConfigKey(GROK_FILTER, filterGroupCounter++));
        for (ConfigDef.ConfigKey configKey : GrokConfig.configDef().configKeys().values()) {
            def.define(new ConfigDef.ConfigKey(
                    configKey.name,
                    configKey.type,
                    configKey.defaultValue,
                    configKey.validator,
                    configKey.importance,
                    configKey.documentation,
                    GROK_FILTER,
                    filterGroupCounter++,
                    configKey.width,
                    configKey.displayName,
                    configKey.dependents,
                    null,
                    true
            ));
        }
        return def;
    }
}
