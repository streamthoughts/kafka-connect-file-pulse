/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.state;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class KafkaFileObjectStateBackingStoreConfig extends AbstractConfig {

    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_CONFIG = "tasks.file.status.storage.topic";
    private static final String TASKS_FILE_STATUS_STORAGE_TOPIC_DOC = "The topic name which is used to report file states.";
    private static final String TASKS_FILE_STATUS_STORAGE_TOPIC_DEFAULT = "connect-file-pulse-status";

    public static final String TASKS_FILE_STATUS_STORAGE_NAME_CONFIG = "tasks.file.status.storage.name";
    private static final String TASKS_FILE_STATUS_STORAGE_NAME_DOC = "The reporter identifier to be used by tasks and connector to report and monitor file progression.";

    public static final String TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG = "tasks.file.status.storage.bootstrap.servers";

    public static final String TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG = "tasks.file.status.storage.consumer.enabled";
    public static final String TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC = "Boolean to indicate if the storage should consume the status topic.";

    /**
     * Creates a new {@link KafkaFileObjectStateBackingStoreConfig} instance.
     *
     * @param originals the configuration properties plus any optional config provider properties; may not be null
     */
    public KafkaFileObjectStateBackingStoreConfig(final Map<?, ?> originals) {
        super(configDef(), originals, false);
    }


    public boolean getTaskStorageConsumerEnabled() {
        return this.getBoolean(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG);
    }

    public String getTaskStorageTopic() {
        return this.getString(TASKS_FILE_STATUS_STORAGE_TOPIC_CONFIG);
    }

    public String getTaskStorageName() {
        return this.getString(TASKS_FILE_STATUS_STORAGE_NAME_CONFIG);
    }

    public Map<String, Object> getTaskStorageConfigs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getInternalBootstrapServers());
        configs.putAll(getInternalKafkaConsumerConfigs());
        configs.putAll(getInternalKafkaProducerConfigs());
        return configs;
    }

    private String getInternalBootstrapServers() {
        return this.getString(TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG);
    }

    private Map<String, Object> getInternalKafkaConsumerConfigs() {
        return this.originalsWithPrefix("tasks.file.status.storage.consumer.");
    }

    private Map<String, Object> getInternalKafkaProducerConfigs() {
        return this.originalsWithPrefix("tasks.file.status.storage.producer.");
    }

    static ConfigDef configDef() {
        return new ConfigDef()
                .define(
                        TASKS_FILE_STATUS_STORAGE_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_DOC
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_STATUS_STORAGE_NAME_DOC
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC
                );

    }
}
