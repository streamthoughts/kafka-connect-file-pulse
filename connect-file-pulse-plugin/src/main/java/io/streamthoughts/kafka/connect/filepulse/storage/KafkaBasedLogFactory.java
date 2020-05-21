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
package io.streamthoughts.kafka.connect.filepulse.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

/**
 */
class KafkaBasedLogFactory {

    private final Map<String, ?> configs;

    /**
     * Creates a new {@link KafkaBasedLogFactory} instance.
     *
     * @param configs the kafka configuration.
     */
    KafkaBasedLogFactory(final Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(configs);
    }

    KafkaBasedLog<String, byte[]> make(final String topic,
                                       final Callback<ConsumerRecord<String, byte[]>> consumedCallback) {
        return new KafkaBasedLog<>(topic,
                newProducerConfigs(), newConsumerConfigs(), consumedCallback, Time.SYSTEM, null);
    }

    private Map<String, Object> newConsumerConfigs() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.putAll(configs);
        consumerProps
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return consumerProps;
    }

    private Map<String, Object> newProducerConfigs() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.putAll(configs);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return producerProps;
    }
}
