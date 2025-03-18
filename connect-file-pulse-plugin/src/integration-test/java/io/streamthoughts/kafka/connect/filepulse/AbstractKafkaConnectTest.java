/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse;

import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class AbstractKafkaConnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseConnectorPluginsIT.class);

    private static final int CONNECT_PORT   = 8083;
    private static final String CONNECT_PLUGIN_PATH     = "/usr/share/java/";
    private static final String CP_CONNECT_IMAGE        = "cp-kafka-connect-base";
    private static final String CONNECTOR_DIR_NAME      = "kafka-connect-filepulse-plugin";

    @Container
    public RedpandaKafkaContainer kafka = new RedpandaKafkaContainer(
            new RedpandaContainerConfig()
                    .withKafkaApiFixedExposedPort(9092)
                    .withAttachContainerOutputLog(true)
                    .withTransactionEnabled(false)
    );

    private GenericContainer<?> containerKafkaConnect;

    @BeforeEach
    public void startContainers() {
        containerKafkaConnect = createConnectWorkerContainer();
        containerKafkaConnect.start();
    }

    @AfterEach
    public void stopContainers() {
        containerKafkaConnect.stop();
    }

    public String getConnectWorker() {
        return containerKafkaConnect.getHost() + ":" + containerKafkaConnect.getFirstMappedPort();
    }

    private GenericContainer<?> createConnectWorkerContainer() {
        return new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka-connect-base:7.9.0"))
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .withNetwork(kafka.getKafkaNetwork())
                .withExposedPorts(CONNECT_PORT)
                .withEnv("CONNECT_REST_PORT", String.valueOf(CONNECT_PORT))
                .withEnv("CONNECT_GROUP_ID", "test")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offset")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "WARN")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", CP_CONNECT_IMAGE)
                .withEnv("CONNECT_PLUGIN_PATH", CONNECT_PLUGIN_PATH)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", kafka.getBootstrapServers())
                .waitingFor(forHttp("/connector-plugins"))
                .withFileSystemBind(getConnectPluginsDistDir(), CONNECT_PLUGIN_PATH + CONNECTOR_DIR_NAME + "/", BindMode.READ_WRITE);
    }

    private static String getConnectPluginsDistDir() {
        return "./target/" + CONNECTOR_DIR_NAME + "-" + Version.getVersion() + "-development/share/java/" + CONNECTOR_DIR_NAME + "/";
    }
}
