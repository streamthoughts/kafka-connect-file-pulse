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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class SplitFilterConfig extends CommonFilterConfig {

    private static final String GROUP_SPLIT_FILTER = "SPLIT_FILTER";

    public static final String MUTATE_SPLIT_CONFIG = "split";
    private static final String MUTATE_SPLIT_DOC = "The comma-separated list of fields to split";

    public static final String MUTATE_SPLIT_SEP_CONFIG = "separator";
    private static final String MUTATE_SPLIT_SEP_DOC = "The separator used for splitting a message field's value to array (default separator : ',')";


    private static final List<Object> DEFAULT_VALUE = Collections.emptyList();

    /**
     * Creates a new {@link SplitFilterConfig} instance.
     *
     * @param originals the configuration.
     */
    public SplitFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }


    public List<String> split() {
        return getList(MUTATE_SPLIT_CONFIG);
    }

    public String splitSeparator() {
        return getString(MUTATE_SPLIT_SEP_CONFIG);
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        MUTATE_SPLIT_SEP_CONFIG,
                        ConfigDef.Type.STRING, ",",
                        ConfigDef.Importance.HIGH,
                        MUTATE_SPLIT_SEP_DOC,
                        GROUP_SPLIT_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MUTATE_SPLIT_SEP_CONFIG
                )
                .define(
                        MUTATE_SPLIT_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        MUTATE_SPLIT_DOC,
                        GROUP_SPLIT_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MUTATE_SPLIT_CONFIG
                );
    }
}
