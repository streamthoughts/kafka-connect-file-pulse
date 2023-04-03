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
package io.streamthoughts.kafka.connect.filepulse;


import com.github.dockerjava.api.command.InspectContainerResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

/**
 * Test container for configuring and starting the Redpanda Kafka broker.
 *
 * See https://vectorized.io/docs/quick-start-docker/
 */
public final class RedpandaKafkaContainer extends GenericContainer<RedpandaKafkaContainer> {

    public static final String VECTORIZED_REDPANDA_LATEST = "vectorized/redpanda:latest";

    private static final Logger LOG = LoggerFactory.getLogger(RedpandaKafkaContainer.class);

    private static final int DEFAULT_KAFKA_API_PORT = 9092;
    private static final int DEFAULT_PROXY_API_PORT = 8082;
    private static final int DEFAULT_ADMIN_API_PORT = 9644;
    private static final int INTERNAL_KAFKA_PORT = 29092;

    private static final String STARTER_SCRIPT = "/var/lib/redpanda/redpanda.sh";


    private final RedpandaContainerConfig config;

    private String hostName = null;

    private Network network;

    /**
     * Creates a new {@link RedpandaKafkaContainer} instance.
     *
     * @param config    the {@link RedpandaContainerConfig}.
     */
    public RedpandaKafkaContainer(final RedpandaContainerConfig config) {
        this(DockerImageName.parse(VECTORIZED_REDPANDA_LATEST), config);
    }

    private RedpandaKafkaContainer(final DockerImageName dockerImageName,
                                   final RedpandaContainerConfig config) {
        super(dockerImageName);
        this.config = config;

        withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sh"));
        withCommand("-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        waitingFor(Wait.forLogMessage(".*Started Kafka API server listening at.*", 1));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);
        copyFileToContainer(Transferable.of( buildStartingScript(), 0777), STARTER_SCRIPT);
    }

    private byte[] buildStartingScript() {
        String command = "#!/bin/bash \n";
        command += String.join(" ",
                "/usr/bin/rpk redpanda start",
                "--smp 1",
                "--overprovisioned",
                "--check=false",
                "--node-id 0",
                "--memory 1G",
                "--reserve-memory 0M"
        );

        command += String.format(" --kafka-addr %s", getKafkaAddresses());
        command += String.format(" --advertise-kafka-addr %s", getKafkaAdvertisedAddresses());

        if (config.isTransactionEnabled()) {
            command += " --set redpanda.enable_idempotence=true";
            command += " --set redpanda.enable_transactions=true";
        }

        return command.getBytes(StandardCharsets.UTF_8);
    }

    private String getKafkaAddresses() {
        return String.join(",",
                "PLAINTEXT://0.0.0.0:" + INTERNAL_KAFKA_PORT,
                "OUTSIDE://0.0.0.0:" + DEFAULT_KAFKA_API_PORT
        );
    }

    private String getKafkaAdvertisedAddresses() {
        return String.join(",",
                String.format("PLAINTEXT://%s:%d", hostName, INTERNAL_KAFKA_PORT),
                String.format("OUTSIDE://%s:%d", getHost(), getMappedPort(DEFAULT_KAFKA_API_PORT))
        );
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected void configure() {
        super.configure();
        hostName = "kafka-" + Base58.randomString(5);
        network = Network.newNetwork();
        setNetwork(network);
        setNetworkAliases(Collections.singletonList(hostName));
        addExposedPorts(DEFAULT_KAFKA_API_PORT, DEFAULT_PROXY_API_PORT, DEFAULT_ADMIN_API_PORT);
        addFixedExposedPort(DEFAULT_KAFKA_API_PORT, config.getKafkaApiFixedExposedPort());

        if (config.isAttachContainerOutputLog()) {
            withLogConsumer(new Slf4jLogConsumer(LOG));
        }
    }

    public Network getKafkaNetwork() {
        return network;
    }

    public String getBootstrapServers() {
        return getKafkaAdvertisedAddresses();
    }

    public void createTopic(final String topicName) {
        try {
            ExecResult execResult = execInContainer(
                    "rpk",
                    "topic",
                    "create",
                    topicName,
                    "--brokers=localhost:" + INTERNAL_KAFKA_PORT
            );
            if (execResult.getExitCode() != 0) {
                throw new RuntimeException(String.format(
                        "Failed to create topic. Command exists with code %d. \n \nStderr: %s\nStdout: %s",
                        execResult.getExitCode(),
                        execResult.getStderr(),
                        execResult.getStdout()
                ));
            } else {
                LOG.info("Created Kafka Topic '{}'. Container Stdout: \n{}", topicName, execResult.getStdout());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}