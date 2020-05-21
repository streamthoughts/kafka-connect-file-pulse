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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

/**
 *
 */
public class TaskConfig extends CommonConfig {

    public static final String FILE_INPUT_PATHS_CONFIG          = "file.input.paths";
    private static final String FILE_INPUT_PATHS_DOC            = "The list of files task must proceed.";

    private static final String OMIT_READ_COMMITTED_FILE_CONFIG = "ignore.committed.offsets";
    private static final String OMIT_READ_COMMITTED_FILE_DOC    = "Boolean indicating whether offsets check has to be performed, to avoid multiple (default : false)";

    private final EnrichedConnectorConfig enrichedConfig;

    static ConfigDef getConf() {
        return CommonConfig.getConf()
                .define(FILE_INPUT_PATHS_CONFIG, ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH, FILE_INPUT_PATHS_DOC)
                .define(OMIT_READ_COMMITTED_FILE_CONFIG, ConfigDef.Type.BOOLEAN, false,
                        ConfigDef.Importance.HIGH, OMIT_READ_COMMITTED_FILE_DOC);
    }

    /**
     * Creates a new {@link TaskConfig} instance.
     * @param originals the original configs.
     */
    public TaskConfig(final Map<String, String> originals) {
        this(getConf(), originals);
    }

    /**
     * Creates a new {@link TaskConfig} instance.
     * @param configDef the configuration definition.
     * @param originals the original configs.
     */
    private TaskConfig(final ConfigDef configDef, final Map<String, String> originals) {
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
        LinkedHashSet<?> uniqueFilterAliases = new LinkedHashSet<>((List<?>) filterAliases);
        for (Object o : uniqueFilterAliases) {
            if (!(o instanceof String)) {
                throw new ConfigException("Item in " + filterAliases + " property is not of type string");
            }
            String alias = (String) o;
            final String prefix = FILTER_CONFIG + "." + alias + ".";
            final String group = FILTERS_GROUP + ": " + alias;
            int orderInGroup = 0;

            final String filterTypeConfig = prefix + "type";
            final ConfigDef.Validator typeValidator = (name, value) -> getConfigDefFromFilter(filterTypeConfig, (Class) value);
            newDef.define(filterTypeConfig, ConfigDef.Type.CLASS, ConfigDef.NO_DEFAULT_VALUE, typeValidator, ConfigDef.Importance.HIGH,
                    "Class for the '" + alias + "' filter.", group, orderInGroup++, ConfigDef.Width.LONG, "Filter type for " + alias);

            final ConfigDef filterConfigDef;
            try {
                final String className = props.get(filterTypeConfig);
                final Class<?> cls = (Class<?>) ConfigDef.parseType(filterTypeConfig, className, ConfigDef.Type.CLASS);
                filterConfigDef = getConfigDefFromFilter(filterTypeConfig, cls);
            } catch (ConfigException e) {
                continue;
            }

            newDef.embed(prefix, group, orderInGroup, filterConfigDef);
        }

        return newDef;
    }

    public List<String> files() {
        return this.getList(FILE_INPUT_PATHS_CONFIG);
    }

    public boolean isReadCommittedFile() {
        return this.getBoolean(OMIT_READ_COMMITTED_FILE_CONFIG);
    }

    public String topic() {
        return this.getString(CommonConfig.OUTPUT_TOPIC_CONFIG);
    }

    public FileInputReader reader() {
        return getConfiguredInstance(CommonConfig.FILE_READER_CLASS_CONFIG, FileInputReader.class);
    }

    public List<RecordFilter> filters() {
        final List<String> filterAliases = getList(FILTER_CONFIG);

        final List<RecordFilter> filters = new ArrayList<>(filterAliases.size());
        for (String alias : filterAliases) {
            final String prefix = FILTER_CONFIG + "." + alias + ".";
            try {
                final RecordFilter filter = getClass(prefix + "type")
                        .asSubclass(RecordFilter.class)
                        .getDeclaredConstructor().newInstance();
                filter.configure(originalsWithPrefix(prefix));
                filters.add(filter);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        return filters;
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
