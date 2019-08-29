/*
 * Copyright 2019 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.offset.OffsetStrategy;
import io.streamthoughts.kafka.connect.filepulse.reader.RowFileInputReader;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class CommonConfig extends AbstractConfig {

    public static final String OUTPUT_TOPIC_CONFIG              = "topic";
    private static final String OUTPUT_TOPIC_DOC                = "The Kafka topic to write the value to.";

    public static final String FILE_READER_CLASS_CONFIG         = "task.reader.class";
    private static final String FILE_READER_CLASS_CONFIG_DOC    = "Class which is used by tasks to read an input file.";

    public static final String OFFSET_STRATEGY_CONFIG           = "offset.strategy";
    private static final String OFFSET_STRATEGY_DOC             = "The strategy to use for building an startPosition from an input file; must be one of [name, path, name+hash].";
    private static final String OFFSET_STRATEGY_DEFAULT         = "name+hash";

    public static final String FILTERS_GROUP                    = "Filters";
    public static final String FILTER_CONFIG                    = "filters";
    private static final String FILTER_DOC                      = "List of filters aliases to apply on each value (order is important).";

    public static final String TASKS_REPORTER_TOPIC             = "internal.kafka.reporter.topic";
    private static final String TASKS_REPORTER_TOPIC_DOC        = "Topic name which is used to report file states.";
    private static final String TASKS_REPORTER_TOPIC_DEFAULT    = "connect-file-pulse-status";

    public static final String INTERNAL_REPORTER_GROUP_ID       = "internal.kafka.reporter.id";
    private static final String INTERNAL_REPORTER_GROUP_ID_DOC  = "Reporter identifier to be used by tasks and connector to report and monitor file progression.";

    public static final String INTERNAL_REPORTER_CLUSTER_BOOTSTRAP_SERVER = "internal.kafka.reporter.bootstrap.servers";

    /**
     * Creates a new {@link CommonConfig} instance.
     * @param definition
     * @param originals
     */
    CommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    static ConfigDef getConf() {
        return new ConfigDef()
                .define(FILE_READER_CLASS_CONFIG, ConfigDef.Type.CLASS,
                        RowFileInputReader.class, ConfigDef.Importance.HIGH,  FILE_READER_CLASS_CONFIG_DOC)

                .define(OUTPUT_TOPIC_CONFIG, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, OUTPUT_TOPIC_DOC)

                .define(OFFSET_STRATEGY_CONFIG, ConfigDef.Type.STRING, OFFSET_STRATEGY_DEFAULT,
                        ConfigDef.Importance.HIGH, OFFSET_STRATEGY_DOC)

                .define(FILTER_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                        ConfigDef.Importance.HIGH, FILTER_DOC, FILTERS_GROUP, -1, ConfigDef.Width.NONE, FILTER_CONFIG)

                .define(TASKS_REPORTER_TOPIC, ConfigDef.Type.STRING, TASKS_REPORTER_TOPIC_DEFAULT,
                        ConfigDef.Importance.HIGH, TASKS_REPORTER_TOPIC_DOC)

                .define(INTERNAL_REPORTER_GROUP_ID, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, INTERNAL_REPORTER_GROUP_ID_DOC)

                .define(INTERNAL_REPORTER_CLUSTER_BOOTSTRAP_SERVER, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC);
    }

    public OffsetStrategy offsetStrategy() {
        return OffsetStrategy.getForLabel(getString(OFFSET_STRATEGY_CONFIG));
    }

    public String getTaskReporterTopic() {
        return this.getString(TASKS_REPORTER_TOPIC);
    }

    public String getTasksReporterGroupId() {
        return this.getString(INTERNAL_REPORTER_GROUP_ID);
    }

    public String getInternalBootstrapServers() {
        return this.getString(INTERNAL_REPORTER_CLUSTER_BOOTSTRAP_SERVER);
    }

    public Map<String, Object> getInternalKafkaConsumerConfigs() {
        return this.originalsWithPrefix("internal.kafka.reporter.consumer.");
    }

    public Map<String, Object> getInternalKafkaProducerConfigs() {
        return this.originalsWithPrefix("internal.kafka.reporter.producer.");
    }

    public Map<String, Object> getInternalKafkaReporterConfig() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getInternalBootstrapServers());
        configs.putAll(getInternalKafkaConsumerConfigs());
        configs.putAll(getInternalKafkaProducerConfigs());

        return configs;
    }
}
