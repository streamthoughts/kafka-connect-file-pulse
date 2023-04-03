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

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.internal.KafkaUtils;
import java.util.Collections;
import java.util.Map;
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

    private final Map<String, ?> producerConfigs;
    private final Map<String, ?> consumerConfigs;

    /**
     * Creates a new {@link KafkaBasedLogFactory} instance.
     *
     * @param producerConfigs configuration options to use when creating the internal producer.
     * @param consumerConfigs configuration options to use when creating the internal consumer.
     */
    KafkaBasedLogFactory(final Map<String, ?> producerConfigs,
                         final Map<String, ?> consumerConfigs) {
        this.producerConfigs = Collections.unmodifiableMap(producerConfigs);
        this.consumerConfigs = Collections.unmodifiableMap(consumerConfigs);
    }

    KafkaBasedLog<String, byte[]> make(final String topic,
                                       final Callback<ConsumerRecord<String, byte[]>> consumedCallback) {
        return new KafkaBasedLog<>(
                topic,
                newProducerConfigs(),
                newConsumerConfigs(),
                consumedCallback,
                Time.SYSTEM,
                null
        );
    }

    private Map<String, Object> newConsumerConfigs() {
        Map<String, Object> clientProps = KafkaUtils.getConsumerConfigs(consumerConfigs);
        clientProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clientProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return clientProps;
    }

    private Map<String, Object> newProducerConfigs() {
        Map<String, Object> clientProps = KafkaUtils.getProducerConfigs(producerConfigs);
        clientProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        clientProps.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return clientProps;
    }
}
