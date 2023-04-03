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

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class ExcludeFilterConfig extends CommonFilterConfig {

    private static final String GROUP_EXCLUDE_FILTER = "EXCLUDE_FILTER";

    public static final String EXCLUDE_FIELDS_CONFIG = "fields";
    private static final String EXCLUDE_FIELDS_DOC = "The comma-separated list of field names to exclude";

    /**
     * Creates a new {@link RenameFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public ExcludeFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }


    public List<String> fields() {
        return getList(EXCLUDE_FIELDS_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        EXCLUDE_FIELDS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        EXCLUDE_FIELDS_DOC,
                        GROUP_EXCLUDE_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        EXCLUDE_FIELDS_CONFIG
                );

    }
}
