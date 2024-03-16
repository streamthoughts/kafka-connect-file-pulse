/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse;

/**
 * Class for configuring Redpanda Kafka Container.
 *
 * @see RedpandaKafkaContainer
 */
public class RedpandaContainerConfig {

    private boolean isTransactionEnabled;

    private int kafkaApiFixedExposedPort = 9092;

    private boolean attachContainerOutputLog = false;

    /**
     * Sets whether the container's output log (i.e., stdout and stderr) should be attached the SL4J log.
     *
     * @param       attachContainerOutputLog {@code true} to attach the container's output log .
     * @return      {@code this}
     */
    public RedpandaContainerConfig withAttachContainerOutputLog(final boolean attachContainerOutputLog) {
        this.attachContainerOutputLog = attachContainerOutputLog;
        return this;
    }

    /**
     * Sets whether Redpanda should be started with transaction and idempotence enabled.
     *
     * @param isTransactionEnabled {@code true} to enable transaction.
     * @return      {@code this}
     */
    public RedpandaContainerConfig withTransactionEnabled(final boolean isTransactionEnabled) {
        this.isTransactionEnabled = isTransactionEnabled;
        return this;
    }

    /**
     * Sets the port to be fixed for exposing the Kafka Api to the host.
     *
     * @param port  the port to be exposed.
     * @return      {@code this}
     */
    public RedpandaContainerConfig withKafkaApiFixedExposedPort(final int port) {
        this.kafkaApiFixedExposedPort = port;
        return this;
    }

    public boolean isAttachContainerOutputLog() {
        return attachContainerOutputLog;
    }

    public boolean isTransactionEnabled() {
        return isTransactionEnabled;
    }

    public int getKafkaApiFixedExposedPort() {
        return kafkaApiFixedExposedPort;
    }
}
