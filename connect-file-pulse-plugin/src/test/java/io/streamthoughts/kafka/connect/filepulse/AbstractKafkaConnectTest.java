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
package io.streamthoughts.kafka.connect.filepulse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

public class AbstractKafkaConnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseIT.class);

    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);

    private static final int ZOOKEEPER_PORT = 2181;

    private static final int KAFKA_PORT     = 9092;

    private static final int CONNECT_PORT   = 8083;

    private static final String ZOOKEEPER_HOSTNAME      = "zookeeper";
    private static final String KAFKA_NETWORK_ALIAS     = "broker";
    private static final String CONNECT_PLUGIN_PATH     = "/usr/share/java/";

    private static final String DOCKER_USERNAME         = "confluentinc";
    private static final String DOCKER_CONFLUENT_TAG    = "6.2.1";
    private static final String CP_ZOOKEEPER_IMAGE      = "cp-zookeeper";
    private static final String CP_KAFKA_IMAGE          = "cp-kafka";
    private static final String CP_CONNECT_IMAGE        = "cp-kafka-connect-base";
    private static final String CONNECTOR_DIR_NAME      = "kafka-connect-filepulse-plugin";

    @ClassRule
    public static Network NETWORK = Network.newNetwork();

    private static final GenericContainer ZOOKEEPER = createZookeeperContainer();

    private static final GenericContainer BROKER = createKafkaBrokerContainer();

    private static final GenericContainer CONNECT = createConnectWorkerContainer();

    @BeforeClass
    public static void startContainers() {
        Stream.of(ZOOKEEPER, BROKER, CONNECT).parallel().forEach(GenericContainer::start);
        CONNECT.followOutput(LOG_CONSUMER);
    }

    @AfterClass
    public static void stopContainers() {
        Stream.of(ZOOKEEPER, BROKER, CONNECT).parallel().forEach(GenericContainer::stop);
    }

    private static GenericContainer createZookeeperContainer() {
        return new FixedHostPortGenericContainer<>(
                getDockerImageName(CP_ZOOKEEPER_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases(ZOOKEEPER_HOSTNAME)
                .withFixedExposedPort(ZOOKEEPER_PORT, ZOOKEEPER_PORT)
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT))
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .withEnv("ZOOKEEPER_SYNC_LIMIT", "2");
    }

    private static GenericContainer createKafkaBrokerContainer() {
        return new FixedHostPortGenericContainer<>(
                getDockerImageName(CP_KAFKA_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases(KAFKA_NETWORK_ALIAS)
                .withFixedExposedPort(KAFKA_PORT, KAFKA_PORT)
                .withEnv("KAFKA_BROKER_ID", "1")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:29092")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:" + KAFKA_PORT + ",BROKER://" + KAFKA_NETWORK_ALIAS + ":29092")
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", ZOOKEEPER_HOSTNAME + ":" + ZOOKEEPER_PORT);
    }

    private static GenericContainer createConnectWorkerContainer() {
        return new FixedHostPortGenericContainer<>(
                getDockerImageName(CP_CONNECT_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases(CP_CONNECT_IMAGE)
                .withFixedExposedPort(CONNECT_PORT, CONNECT_PORT)
                .withEnv("CONNECT_REST_PORT", String.valueOf(CONNECT_PORT))
                .withEnv("CONNECT_GROUP_ID", "testGivenDefaultProperties")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "testGivenDefaultProperties-configDef")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "testGivenDefaultProperties-offset")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "testGivenDefaultProperties-status")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "WARN")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", CP_CONNECT_IMAGE)
                .withEnv("CONNECT_PLUGIN_PATH", CONNECT_PLUGIN_PATH)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", KAFKA_NETWORK_ALIAS + ":29092")
                .waitingFor(forHttp("/connector-plugins"))
                .withFileSystemBind(getConnectPluginsDistDir(), CONNECT_PLUGIN_PATH + CONNECTOR_DIR_NAME + "/", BindMode.READ_WRITE);
    }

    private static String getConnectPluginsDistDir() {
        return "./target/" + CONNECTOR_DIR_NAME + "-" + Version.getVersion() + "-development/share/java/" + CONNECTOR_DIR_NAME + "/";
    }

    private static String getDockerImageName(final String name) {
        return DOCKER_USERNAME + "/" + name + ":" + DOCKER_CONFLUENT_TAG;
    }

    protected String getBootstrapServer() {
        return BROKER.getIpAddress() + ":" + BROKER.getMappedPort(KAFKA_PORT);
    }

    protected String getConnectWorker() {
        return CONNECT.getIpAddress() + ":" + CONNECT.getMappedPort(CONNECT_PORT);
    }


    private void testKafkaFunctionality(String bootstrapServers) {
        String topicName = "testGivenDefaultProperties-topic";

        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "hello");
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        try (
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        producerConfigs,
                        new StringSerializer(),
                        new StringSerializer()
                );

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        consumerConfigs,
                        new StringDeserializer(),
                        new StringDeserializer()
                );
        ) {

            consumer.subscribe(Collections.singletonList(topicName));

            producer.send(new ProducerRecord<>(topicName, "testcontainers", "rulezzz"),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            System.out.println(metadata);
                        }
                    });
            producer.send(new ProducerRecord<>(topicName, "testcontainers", "rulezzz"),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            System.out.println(metadata);
                        }
                    });
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(System.out::println);

            }
        }
    }
}
