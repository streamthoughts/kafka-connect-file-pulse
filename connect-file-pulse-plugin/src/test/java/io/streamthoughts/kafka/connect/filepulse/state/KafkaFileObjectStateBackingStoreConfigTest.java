/*
 * Copyright 2023 StreamThoughts.
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

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaFileObjectStateBackingStoreConfigTest {

    @Test
    void should_return_default_topic_configs_given_valid_props() {
        // GIVEN
        var config = new KafkaFileObjectStateBackingStoreConfig(Map.of(
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG, "???"
        ));

        // WHEN
        String topic = config.getTaskStorageTopic();

        // THEN
        Assertions.assertEquals("connect-file-pulse-status", topic);
    }

    @Test
    void should_return_override_topic_configs_given_valid_props() {
        // GIVEN
        var config = new KafkaFileObjectStateBackingStoreConfig(Map.of(
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_TOPIC_CONFIG, "test",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG, "???"
        ));

        // WHEN
        String topic = config.getTaskStorageTopic();

        // THEN
        Assertions.assertEquals("test", topic);
    }

    @Test
    void should_return_name_configs_given_valid_props() {
        // GIVEN
        var config = new KafkaFileObjectStateBackingStoreConfig(Map.of(
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, "test",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG, "???"
        ));

        // WHEN
        String name = config.getTaskStorageName();

        // THEN
        Assertions.assertEquals("test", name);
    }

    @Test
    void should_return_consumer_configs_given_valid_props() {
        // GIVEN
        var config = new KafkaFileObjectStateBackingStoreConfig(Map.of(
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG, "???",
                "tasks.file.status.storage.random", "???",
                "tasks.file.status.storage." + ConsumerConfig.CLIENT_ID_CONFIG, "???",
                "tasks.file.status.storage.consumer." + ConsumerConfig.GROUP_ID_CONFIG, "???"
        ));
        
        // WHEN
        Map<String, Object> consumerConfigs = config.getConsumerTaskStorageConfigs();

        // THEN
        Assertions.assertEquals(3, consumerConfigs.size());
        Assertions.assertNotNull(consumerConfigs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertNotNull(consumerConfigs.get(ConsumerConfig.CLIENT_ID_CONFIG));
        Assertions.assertNotNull(consumerConfigs.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    void should_return_producer_configs_given_valid_props() {
        // GIVEN
        var config = new KafkaFileObjectStateBackingStoreConfig(Map.of(
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG, "???",
                "tasks.file.status.storage.random", "???",
                "tasks.file.status.storage." + ProducerConfig.CLIENT_ID_CONFIG, "???",
                "tasks.file.status.storage.producer." + ProducerConfig.ACKS_CONFIG, "???"
        ));

        // WHEN
        Map<String, Object> producerConfigs = config.getProducerTaskStorageConfigs();

        // THEN
        Assertions.assertEquals(3, producerConfigs.size());
        Assertions.assertNotNull(producerConfigs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertNotNull(producerConfigs.get(ProducerConfig.CLIENT_ID_CONFIG));
        Assertions.assertNotNull(producerConfigs.get(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    void should_return_topic_creation_disable_configs_given_false_props() {
        // GIVEN
        var config = new KafkaFileObjectStateBackingStoreConfig(Map.of(
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_CONFIG, false
        ));

        // WHEN
        boolean enable = config.isTopicCreationEnable();

        // THEN
        Assertions.assertFalse(enable);
    }

    @Test
    void should_return_topic_creation_enable_configs_given_true_props() {
        // GIVEN
        var config = new KafkaFileObjectStateBackingStoreConfig(Map.of(
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_BOOTSTRAP_SERVERS_CONFIG, "???",
                KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_TOPIC_CREATION_ENABLE_CONFIG, true
        ));

        // WHEN
        boolean enable = config.isTopicCreationEnable();

        // THEN
        Assertions.assertTrue(enable);
    }
}