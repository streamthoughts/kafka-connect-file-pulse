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

import io.streamthoughts.kafka.connect.filepulse.filter.condition.ExpressionFilterCondition;
import io.streamthoughts.kafka.connect.filepulse.filter.condition.FilterCondition;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class CommonFilterConfig extends AbstractConfig {

    private static final String GROUP_FILTER = "Filter";

    public static final String ON_FAILURE_CONFIG = "withOnFailure";
    public static final String ON_FAILURE_DOC = "List of filters aliases to apply on each value after failure (order is important).";

    public static final String CONDITION_CONFIG = "if";
    public static final String CONDITION_DOC = "Condition to apply the filter on the current record.";

    public static final String CONDITION_NOT_CONFIG = "invert";
    public static final String CONDITION_NOT_DOC = "Invert the boolean value return from the filter condition.";

    public static final String IGNORE_FAILURE_CONFIG = "ignoreFailure";
    public static final String IGNORE_FAILURE_DOC = "Ignore failure and continue pipeline filters";

    public static final String FILTER_OVERWRITE_CONFIG = "overwrite";
    public static final String FILTER_OVERWRITE_DOC = "The fields to overwrite.";

    public static final String FILTER_SOURCE_FIELD_CONFIG = "source";
    private static final String FILTER_SOURCE_FIELD_DOC = "The input field on which to apply the filter (default: message).";

    /**
     * Creates a new {@link CommonFilterConfig} instance.
     *
     * @param originals the originals configuration.
     */
    public CommonFilterConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    /**
     * Creates a new {@link CommonFilterConfig} instance.
     *
     * @param def       the {@link ConfigDef} instance.
     * @param originals the originals configuration.
     */
    public CommonFilterConfig(final ConfigDef def, final Map<?, ?> originals) {
        this(def, originals, true);
    }

    /**
     * Creates a new {@link CommonFilterConfig} instance.
     *
     * @param def       the {@link ConfigDef} instance.
     * @param originals the originals configuration.
     */
    public CommonFilterConfig(final ConfigDef def, final Map<?, ?> originals, final boolean doLog) {
        super(def, originals, doLog);
    }

    public FilterCondition condition() {
        final String strCondition = getString(CONDITION_CONFIG);

        Boolean revert = getBoolean(CONDITION_NOT_CONFIG);
        if (strCondition == null) {
            return FilterCondition.TRUE;
        }

        ExpressionFilterCondition condition = new ExpressionFilterCondition(strCondition);

        return revert ? FilterCondition.revert(condition) : condition;
    }

    public boolean ignoreFailure() {
        return getBoolean(IGNORE_FAILURE_CONFIG);
    }

    public List<String> onFailure() {
        final List<String> aliases = getList(ON_FAILURE_CONFIG);
        return aliases == null ? Collections.emptyList() : aliases;
    }

    public Set<String> overwrite() {
        return new HashSet<>(getList(CommonFilterConfig.FILTER_OVERWRITE_CONFIG));
    }

    public String source() {
        return getString(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG);
    }

    public static ConfigDef.ConfigKey getOverwriteConfigKey(final String group, final int groupCounter) {
        return new ConfigDef.ConfigKey(
                CommonFilterConfig.FILTER_OVERWRITE_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                null,
                ConfigDef.Importance.HIGH,
                CommonFilterConfig.FILTER_OVERWRITE_DOC,
                group,
                groupCounter,
                ConfigDef.Width.NONE,
                CommonFilterConfig.FILTER_OVERWRITE_CONFIG,
                Collections.<String>emptyList(),
                null,
                true
        );
    }

    public static ConfigDef.ConfigKey getSourceConfigKey() {
        return getSourceConfigKey(null, -1);
    }

    public static ConfigDef.ConfigKey getSourceConfigKey(final String group, final int groupCounter) {
        return new ConfigDef.ConfigKey(
                CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG,
                ConfigDef.Type.STRING,
                TypedFileRecord.DEFAULT_MESSAGE_FIELD,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                CommonFilterConfig.FILTER_SOURCE_FIELD_DOC,
                group,
                groupCounter,
                ConfigDef.Width.NONE,
                CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG,
                Collections.<String>emptyList(),
                null,
                true
        );
    }

    public static ConfigDef configDef() {
        int filterGroupCounter = 0;
        return new ConfigDef()
                .define(
                        ON_FAILURE_CONFIG,
                        ConfigDef.Type.LIST,
                        null,
                        ConfigDef.Importance.HIGH,
                        ON_FAILURE_DOC,
                        GROUP_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        ON_FAILURE_CONFIG
                )
                .define(
                        IGNORE_FAILURE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        IGNORE_FAILURE_DOC,
                        GROUP_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        IGNORE_FAILURE_CONFIG
                )
                .define(
                        CONDITION_NOT_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        CONDITION_NOT_DOC,
                        GROUP_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        CONDITION_NOT_CONFIG
                )
                .define(
                        CONDITION_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        CONDITION_DOC,
                        GROUP_FILTER,
                        filterGroupCounter++,
                        ConfigDef.Width.NONE,
                        CONDITION_CONFIG
                );
    }
}
