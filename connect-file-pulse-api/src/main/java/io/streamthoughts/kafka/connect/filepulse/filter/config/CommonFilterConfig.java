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
package io.streamthoughts.kafka.connect.filepulse.filter.config;

import io.streamthoughts.kafka.connect.filepulse.filter.DefaultRecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.filter.condition.ExpressionFilterCondition;
import io.streamthoughts.kafka.connect.filepulse.filter.condition.FilterCondition;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CommonFilterConfig extends AbstractConfig {

    public static final String ON_FAILURE_CONFIG          = "withOnFailure";
    public static final String ON_FAILURE_DOC             = "List of filters aliases to apply on each data after failure (order is important).";

    public static final String CONDITION_CONFIG           = "if";
    public static final String CONDITION_DOC              = "Condition to apply the filter on the current record.";

    public static final String CONDITION_NOT_CONFIG       = "invert";
    public static final String CONDITION_NOT_DOC          = "Invert the boolean value return from the filter condition.";

    public static final String IGNORE_FAILURE_CONFIG      = "ignoreFailure";
    public static final String IGNORE_FAILURE_DOC         = "Ignore failure and continue pipeline filters";

    public static final String FILTER_OVERWRITE_CONFIG    = "overwrite";
    public static final String FILTER_OVERWRITE_DOC       = "The fields to overwrite.";

    public static final String FILTER_SOURCE_FIELD_CONFIG = "source";
    private static final String FILTER_SOURCE_FIELD_DOC   = "The input field on which to apply the filter.";

    /**
     * Creates a new {@link CommonFilterConfig} instance.
     * @param originals the originals configuration.
     */
    public CommonFilterConfig(final Map<?, ?> originals) {
        super(configDef() , originals);
    }

    /**
     * Creates a new {@link CommonFilterConfig} instance.
     * @param originals the originals configuration.
     */
    public CommonFilterConfig(final ConfigDef def, final Map<?, ?> originals) {
        super(def, originals);
    }

    public FilterCondition condition() {
        final String strCondition = getString(CONDITION_CONFIG);

        Boolean revert = getBoolean(CONDITION_NOT_CONFIG);
        if (strCondition == null) {
            return FilterCondition.TRUE;
        }

        ExpressionFilterCondition condition = new ExpressionFilterCondition(strCondition);

        return revert ? FilterCondition.revert(condition) : condition ;
    }

    public boolean ignoreFailure() {
        return getBoolean(IGNORE_FAILURE_CONFIG);
    }

    public RecordFilterPipeline<FileInputRecord> onFailure() {
        final List<String> filterAliases = getList(ON_FAILURE_CONFIG);

        if (filterAliases == null) return null;

        final List<RecordFilter> filters = new ArrayList<>(filterAliases.size());
        for (String alias : filterAliases) {
            final String prefix = "filters." + alias + ".";
            try {
                @SuppressWarnings("unchecked")
                final RecordFilter filter = getClass(prefix + "type")
                        .asSubclass(RecordFilter.class)
                        .getDeclaredConstructor().newInstance();
                filter.configure(originalsWithPrefix(prefix));
                filters.add(filter);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }
        return filters.isEmpty() ? null : new DefaultRecordFilterPipeline(filters);
    }

    public static ConfigDef withOverwrite(final ConfigDef def) {
        return def.define(CommonFilterConfig.FILTER_OVERWRITE_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                ConfigDef.Importance.HIGH, CommonFilterConfig.FILTER_OVERWRITE_DOC);
    }

    public static ConfigDef withSource(final ConfigDef def) {
        return def.define(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG, ConfigDef.Type.STRING, FileInputData.DEFAULT_MESSAGE_FIELD,
                ConfigDef.Importance.HIGH, CommonFilterConfig.FILTER_SOURCE_FIELD_DOC);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(ON_FAILURE_CONFIG, ConfigDef.Type.LIST, null,
                        ConfigDef.Importance.HIGH, ON_FAILURE_DOC)
                .define(IGNORE_FAILURE_CONFIG, ConfigDef.Type.BOOLEAN, false,
                        ConfigDef.Importance.HIGH, IGNORE_FAILURE_DOC)
                .define(CONDITION_NOT_CONFIG, ConfigDef.Type.BOOLEAN, false,
                        ConfigDef.Importance.HIGH, CONDITION_NOT_DOC)
                .define(CONDITION_CONFIG, ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.HIGH, CONDITION_DOC);
    }
}
