/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaUtils {

    /**
     * Return only relevant key-value relevant for creating new AdminClient from the given map.
     *
     * @param configs   the config key-value map.
     * @return          the map with only configs properties relevant for AdminClient.
     */
    public static Map<String, Object> getAdminClientConfigs(final Map<String, ?> configs) {
        return getConfigsForKeys(configs, AdminClientConfig.configNames());
    }
    /**
     * Return only relevant key-value relevant for creating new KafkaConsumer from the given map.
     *
     * @param configs   the config key-value map.
     * @return          the map with only configs properties relevant for KafkaConsumer.
     */
    public static Map<String, Object> getConsumerConfigs(final Map<String, ?> configs) {
        return getConfigsForKeys(configs, ConsumerConfig.configNames());
    }
    /**
     * Return only relevant key-value relevant for creating new KafkaProducer from the given map.
     *
     * @param configs   the config key-value map.
     * @return          the map with only configs properties relevant for KafkaProducer.
     */
    public static Map<String, Object> getProducerConfigs(final Map<String, ?> configs) {
        return getConfigsForKeys(configs, ProducerConfig.configNames());
    }

    private static Map<String, Object> getConfigsForKeys(final Map<String, ?> configs,
                                                         final Set<String> keys) {
        final Map<String, Object> parsed = new HashMap<>();
        for (final String configName : keys) {
            if (configs.containsKey(configName)) {
                parsed.put(configName, configs.get(configName));
            }
        }
        return parsed;
    }
}
