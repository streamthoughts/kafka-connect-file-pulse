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
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaUtilsTest {


    @Test
    void should_return_producer_client_props() {
        Map<String, String> configs = Map.of(
                "foo", "???",
                "bar", "???",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "???",
                ProducerConfig.ACKS_CONFIG, "???"
        );
        Map<String, Object> producerConfigs = KafkaUtils.getProducerConfigs(configs);
        Assertions.assertEquals(2, producerConfigs.size());
        Assertions.assertNotNull(producerConfigs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertNotNull(producerConfigs.get(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    void should_return_consumer_client_props() {
        Map<String, String> configs = Map.of(
                "foo", "???",
                "bar", "???",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "???",
                ConsumerConfig.GROUP_ID_CONFIG, "???"
        );
        Map<String, Object> consumerConfigs = KafkaUtils.getConsumerConfigs(configs);
        Assertions.assertEquals(2, consumerConfigs.size());
        Assertions.assertNotNull(consumerConfigs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertNotNull(consumerConfigs.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    void should_return_admin_client_props() {
        Map<String, String> configs = Map.of(
                "foo", "???",
                "bar", "???",
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "???"
        );
        Map<String, Object> clientConfigs = KafkaUtils.getAdminClientConfigs(configs);
        Assertions.assertEquals(1, clientConfigs.size());
    }
}