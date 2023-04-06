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

import io.streamthoughts.kafka.connect.filepulse.internal.KafkaUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class KafkaFileObjectStateBackingStoreConfig extends AbstractConfig {

    private static final String GROUP = "KafkaFileObjectStateBackingStore";

    public static final String TASKS_FILE_STATUS_STORAGE_PREFIX = "tasks.file.status.storage.";

    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_CONFIG = TASKS_FILE_STATUS_STORAGE_PREFIX + "topic";
    private static final String TASKS_FILE_STATUS_STORAGE_TOPIC_DOC = "The topic name which is used to report file states.";
    private static final String TASKS_FILE_STATUS_STORAGE_TOPIC_DEFAULT = "connect-file-pulse-status";

    public static final String TASKS_FILE_STATUS_STORAGE_NAME_CONFIG = TASKS_FILE_STATUS_STORAGE_PREFIX + "name";
    private static final String TASKS_FILE_STATUS_STORAGE_NAME_DOC = "The reporter identifier to be used by tasks and connector to report and monitor file progression.";

    public static final String TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG = TASKS_FILE_STATUS_STORAGE_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    public static final String TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG = TASKS_FILE_STATUS_STORAGE_PREFIX + "consumer.enabled";
    public static final String TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC = "Boolean to indicate if the storage should consume the status topic.";

    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_PARTITIONS_CONFIG = TASKS_FILE_STATUS_STORAGE_PREFIX + "topic.partitions";
    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_PARTITIONS_DOC = "The number of partitions to be used for the status storage topic.";

    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG = TASKS_FILE_STATUS_STORAGE_PREFIX + "topic.replication.factor";
    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor to be used for the status storage topic.";

    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_CONFIG = TASKS_FILE_STATUS_STORAGE_PREFIX + "topic.creation.enable";
    public static final String TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_DOC = "Boolean to indicate if the status storage topic should be automatically created.";


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
    public boolean isTopicCreationEnable() {
        return this.getBoolean(TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_CONFIG);
    }

    public Map<String, Object> getConsumerTaskStorageConfigs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getInternalBootstrapServers());
        configs.putAll(getInternalKafkaConsumerConfigs());
        return configs;
    }

    public Map<String, Object> getProducerTaskStorageConfigs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getInternalBootstrapServers());
        configs.putAll(getInternalKafkaProducerConfigs());
        return configs;
    }

    public Map<String, Object> getAdminClientTaskStorageConfigs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getInternalBootstrapServers());
        configs.putAll(getInternalKafkaAdminClientConfigs());
        return configs;
    }

    private String getInternalBootstrapServers() {
        return this.getString(TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG);
    }

    private Map<String, Object> getInternalKafkaAdminClientConfigs() {
        Map<String, Object> originals = originalsWithPrefix(TASKS_FILE_STATUS_STORAGE_PREFIX, true);
        Map<String, Object> adminClientConfigs = new HashMap<>(originals);
        adminClientConfigs.putAll(originalsWithPrefix(TASKS_FILE_STATUS_STORAGE_PREFIX + "admin."));
        return KafkaUtils.getAdminClientConfigs(adminClientConfigs);
    }

    private Map<String, Object> getInternalKafkaConsumerConfigs() {
        Map<String, Object> originals = originalsWithPrefix(TASKS_FILE_STATUS_STORAGE_PREFIX, true);
        Map<String, Object> consumerConfigs = new HashMap<>(originals);
        consumerConfigs.putAll(originalsWithPrefix(TASKS_FILE_STATUS_STORAGE_PREFIX + "consumer."));
        return KafkaUtils.getConsumerConfigs(consumerConfigs);
    }

    private Map<String, Object> getInternalKafkaProducerConfigs() {
        Map<String, Object> originals = originalsWithPrefix(TASKS_FILE_STATUS_STORAGE_PREFIX, true);
        Map<String, Object> producerConfigs = new HashMap<>(originals);
        producerConfigs.putAll(originalsWithPrefix(TASKS_FILE_STATUS_STORAGE_PREFIX + "producer."));
        return KafkaUtils.getProducerConfigs(producerConfigs);
    }

    Optional<Integer> getTopicPartitions() {
        return Optional.ofNullable(getInt(TASKS_FILE_STATUS_STORAGE_TOPIC_PARTITIONS_CONFIG));
    }

    Optional<Short> getReplicationFactor() {
        return Optional.ofNullable(getShort(TASKS_FILE_STATUS_STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG));
    }

    static ConfigDef configDef() {
        int groupCounter = 0;
        return new ConfigDef()
                .define(
                        TASKS_FILE_STATUS_STORAGE_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_CONFIG
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_CONFIG
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_REPLICATION_FACTOR_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_TOPIC_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_PARTITIONS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_TOPIC_PARTITIONS_CONFIG
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_STATUS_STORAGE_NAME_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_NAME_CONFIG
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG
                );

    }
}
