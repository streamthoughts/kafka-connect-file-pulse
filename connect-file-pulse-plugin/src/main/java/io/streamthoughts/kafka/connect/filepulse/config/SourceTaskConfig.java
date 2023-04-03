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

import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultTaskFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.fs.TaskFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

/**
 *
 */
public class SourceTaskConfig extends CommonSourceConfig {

    public static final String FILE_URIS_PROVIDER_CONFIG = "file.uris.provider";
    private static final String FILE_URIS_PROVIDER_DOC    = "The FileURIProvider class to be used for retrieving the file URIs to process.";

    private static final String OMIT_READ_COMMITTED_FILE_CONFIG = "ignore.committed.offsets";
    private static final String OMIT_READ_COMMITTED_FILE_DOC = "Should a task ignore committed offsets while scheduling a file (default : false).";

    public static final String TASK_GENERATION_ID = "task.generation.id";
    private static final String TASK_GENERATION_DOC = "The task configuration generation id.";

    private final EnrichedConnectorConfig enrichedConfig;

    static ConfigDef getConf() {
        return CommonSourceConfig.getConfigDef()
                .define(
                        FILE_URIS_PROVIDER_CONFIG,
                        ConfigDef.Type.CLASS,
                        DefaultTaskFileURIProvider.class,
                        ConfigDef.Importance.HIGH,
                        FILE_URIS_PROVIDER_DOC
                )
                .define(
                        OMIT_READ_COMMITTED_FILE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        OMIT_READ_COMMITTED_FILE_DOC
                )
                .define(
                        TASK_GENERATION_ID,
                        ConfigDef.Type.INT,
                        0,
                        ConfigDef.Importance.LOW,
                        TASK_GENERATION_DOC
                );
    }

    /**
     * Creates a new {@link SourceTaskConfig} instance.
     *
     * @param originals the original configs.
     */
    public SourceTaskConfig(final Map<String, String> originals) {
        this(getConf(), originals);
    }

    /**
     * Creates a new {@link SourceTaskConfig} instance.
     *
     * @param configDef the configuration definition.
     * @param originals the original configs.
     */
    private SourceTaskConfig(final ConfigDef configDef, final Map<String, String> originals) {
        super(getConf(), originals);
        enrichedConfig = new EnrichedConnectorConfig(
                enrich(configDef, originals),
                originals
        );
    }

    @Override
    public Object get(String key) {
        return enrichedConfig.get(key);
    }

    private static ConfigDef enrich(final ConfigDef baseConfigDef, final Map<String, String> props) {
        Object filterAliases = ConfigDef.parseType(FILTER_CONFIG, props.get(FILTER_CONFIG), ConfigDef.Type.LIST);
        if (!(filterAliases instanceof List)) {
            return baseConfigDef;
        }

        final ConfigDef newDef = new ConfigDef(baseConfigDef);

        LinkedHashSet<String> uniqueFilterAliases = new LinkedHashSet<>();
        lookupAllFilterAliases(props, (List<?>) filterAliases, uniqueFilterAliases);

        uniqueFilterAliases.forEach(alias -> addConfigDefForFilter(props, newDef, alias));

        return newDef;
    }

    private static void lookupAllFilterAliases(final Map<String, String> props,
                                               final Collection<?> filterAliases,
                                               final LinkedHashSet<String> accumulator) {
        for (Object alias : filterAliases) {
            if (!(alias instanceof String)) {
                throw new ConfigException("Item in " + filterAliases + " property is not of type string");
            }
            accumulator.add((String) alias);
            final String failure = props.get(FILTER_CONFIG + "." + alias + "." + CommonFilterConfig.ON_FAILURE_CONFIG);
            if (failure != null && !failure.isEmpty()) {
                final Set<String> filters = Arrays.stream(failure
                        .split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
                lookupAllFilterAliases(props, filters, accumulator);
            }
        }
    }

    private static void addConfigDefForFilter(final Map<String, String> props,
                                              final ConfigDef newDef,
                                              final String alias) {
        final String prefix = FILTER_CONFIG + "." + alias + ".";
        final String group = FILTERS_GROUP + ":" + alias;
        int orderInGroup = 0;

        final String filterTypeConfig = prefix + "type";
        final ConfigDef.Validator typeValidator = (name, value) -> getConfigDefFromFilter(filterTypeConfig, (Class<?>) value);
        newDef.define(filterTypeConfig,
                ConfigDef.Type.CLASS,
                ConfigDef.NO_DEFAULT_VALUE,
                typeValidator,
                ConfigDef.Importance.HIGH,
                "Class for the '" + alias + "' filter.",
                group,
                orderInGroup++,
                ConfigDef.Width.LONG,
                "Filter type for " + alias
        );

        final ConfigDef filterConfigDef;
        try {
            final String className = props.get(filterTypeConfig);
            final Class<?> cls = (Class<?>) ConfigDef.parseType(filterTypeConfig, className, ConfigDef.Type.CLASS);
            filterConfigDef = getConfigDefFromFilter(filterTypeConfig, cls);
        } catch (ConfigException e) {
            return;
        }
        newDef.embed(prefix, group, orderInGroup, filterConfigDef);
    }

    public TaskFileURIProvider getFileURIProvider() {
        return this.getConfiguredInstance(FILE_URIS_PROVIDER_CONFIG, TaskFileURIProvider.class);
    }

    public boolean isReadCommittedFile() {
        return this.getBoolean(OMIT_READ_COMMITTED_FILE_CONFIG);
    }

    public String topic() {
        return this.getString(CommonSourceConfig.OUTPUT_TOPIC_CONFIG);
    }

    public FileInputReader reader() {
        return getConfiguredInstance(CommonSourceConfig.TASKS_FILE_READER_CLASS_CONFIG, FileInputReader.class);
    }

    public int getTaskGenerationId() {
        return this.getInt(TASK_GENERATION_ID);
    }

    public List<RecordFilter> filters() {
        final List<String> filterAliases = getList(FILTER_CONFIG);
        final List<RecordFilter> filters = new ArrayList<>(filterAliases.size());
        for (String alias : filterAliases) {
            filters.add(filterByAlias(alias));
        }
        return filters;
    }

    public RecordFilter filterByAlias(final String alias) {
        final String prefix = FILTER_CONFIG + "." + alias + ".";
        try {
            final RecordFilter filter = getClass(prefix + "type")
                    .asSubclass(RecordFilter.class)
                    .getDeclaredConstructor().newInstance();
            filter.configure(originalsWithPrefix(prefix), this::filterByAlias);
            return filter;
        } catch (Exception e) {
            throw new ConnectException("Failed to create filter with alias '" + alias + "'", e);
        }
    }

    /**
     * Return {@link ConfigDef} from {@code filterClass}, which is expected to be a non-null {@code Class<RecordFilter>},
     * by instantiating it and invoking {@link RecordFilter#configDef()}.
     */
    private static ConfigDef getConfigDefFromFilter(final String key, final Class<?> filterClass) {
        if (filterClass == null || !RecordFilter.class.isAssignableFrom(filterClass)) {
            throw new ConfigException(key, String.valueOf(filterClass), "Not a RecordFilter");
        }
        RecordFilter filter;
        try {
            filter = filterClass.asSubclass(RecordFilter.class).getConstructor().newInstance();
        } catch (Exception e) {
            throw new ConfigException(key, String.valueOf(filterClass), "Error getting configDef definition from RecordFilter: " + e.getMessage());
        }
        ConfigDef configDef = filter.configDef();
        if (null == configDef) {
            throw new ConnectException(
                    String.format(
                            "%s.configDef() must return a ConfigDef that is not null.",
                            filterClass.getName()
                    )
            );
        }
        return configDef;
    }

    private static class EnrichedConnectorConfig extends AbstractConfig {

        /**
         * Creates a new {@link EnrichedConnectorConfig} instance.
         *
         * @param configDef the configuration definition.
         * @param props     the original configurations.
         */
        EnrichedConnectorConfig(final ConfigDef configDef,
                                final Map<String, String> props) {
            super(configDef, props);
        }

        @Override
        public Object get(final String key) {
            return super.get(key);
        }
    }

}
