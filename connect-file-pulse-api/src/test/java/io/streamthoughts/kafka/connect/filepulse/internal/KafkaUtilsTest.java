/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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