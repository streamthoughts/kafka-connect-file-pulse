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
