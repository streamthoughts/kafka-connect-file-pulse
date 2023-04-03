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

import static io.streamthoughts.kafka.connect.transform.GrokConfig.GROK_PATTERNS_DIR_CONFIG;
import static io.streamthoughts.kafka.connect.transform.GrokConfig.GROK_PATTERN_CONFIG;
import static io.streamthoughts.kafka.connect.transform.GrokConfig.GROK_PATTERN_DEFINITIONS_CONFIG;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;

public class MultiRowFilterConfig extends CommonFilterConfig {

    private static final String GROUP_MULTIROW_FILTER = "MULTIROW_FILTER";

    public static final String MULTI_ROW_NEGATE_CONFIG = "negate";
    private static final String MULTI_ROW_NEGATE_DOC = "Negate the regexp pattern (if not matched).";

    public static final String MULTI_ROW_LINE_SEPARATOR_CONFIG = "separator";
    public static final String MULTI_ROW_LINE_SEPARATOR_DEFAULT = "\\n";
    private static final String MULTI_ROW_LINE_SEPARATOR_DOC = "The character to be used to concat multi lines";

    /**
     * Creates a new {@link MultiRowFilterConfig} instance.
     *
     * @param originals the reader configuration.
     */
    public MultiRowFilterConfig(final Map<String, ?> originals) {
        super(configDef(), originals);
    }

    public boolean negate() {
        return getBoolean(MULTI_ROW_NEGATE_CONFIG);
    }

    public String separator() {
        return getString(MULTI_ROW_LINE_SEPARATOR_CONFIG);
    }

    public String pattern() {
        return getString(GROK_PATTERN_CONFIG);
    }

    public List<String> patternDefinitions() {
        return this.getList(GROK_PATTERN_DEFINITIONS_CONFIG);
    }

    public Collection<File> patternsDir() {
        return this.getList(GROK_PATTERNS_DIR_CONFIG)
                .stream()
                .map(File::new)
                .collect(Collectors.toList());
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef(CommonFilterConfig.configDef())
                .define(
                        MULTI_ROW_NEGATE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        MULTI_ROW_NEGATE_DOC,
                        GROUP_MULTIROW_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MULTI_ROW_NEGATE_CONFIG
                )
                .define(
                        GROK_PATTERN_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "The Grok pattern to match multiple lines.",
                        GROUP_MULTIROW_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        GROK_PATTERN_CONFIG
                )
                .define(
                        MULTI_ROW_LINE_SEPARATOR_CONFIG,
                        ConfigDef.Type.STRING,
                        MULTI_ROW_LINE_SEPARATOR_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        MULTI_ROW_LINE_SEPARATOR_DOC,
                        GROUP_MULTIROW_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        MULTI_ROW_LINE_SEPARATOR_CONFIG
                )
                .define(GROK_PATTERNS_DIR_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.MEDIUM,
                        "List of user-defined pattern directories",
                        GROUP_MULTIROW_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        GROK_PATTERNS_DIR_CONFIG
                )

                .define(
                        GROK_PATTERN_DEFINITIONS_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.MEDIUM,
                        "Custom pattern definitions",
                        GROUP_MULTIROW_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        GROK_PATTERN_DEFINITIONS_CONFIG
                );
    }
}
