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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStateBackingStore<T> implements StateBackingStore<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStateBackingStore.class);

    private static final Duration DEFAULT_READ_TO_END_TIMEOUT = Duration.ofSeconds(30);

    private static final String GROUP_STATE_SEPARATOR = ".";

    private final KafkaBasedLog<String, byte[]> kafkaLog;

    private final Object lock = new Object();

    private final String groupId;
    private final AtomicLong offset = new AtomicLong(-1);
    private final Map<String, T> states = new HashMap<>();
    private final StateSerde<T> serde;
    private final String keyPrefix;
    private final boolean consumerEnabled;

    private volatile Status status = Status.CREATED;
    private StateBackingStore.UpdateListener<T> updateListener;

    /**
     * Creates a new {@link KafkaStateBackingStore} instance.
     *
     * @param topic     the topic back store.
     * @param keyPrefix the key-prefix.
     * @param groupId   the group attached to the backing topic (i.e., the connector-name).
     * @param producerProps   the kafka producer properties.
     * @param consumerProps   the kafka consumer properties.
     * @param serde     the {@link StateSerde}.
     */
    public KafkaStateBackingStore(final String topic,
                                  final String keyPrefix,
                                  final String groupId,
                                  final Map<String, ?> producerProps,
                                  final Map<String, ?> consumerProps,
                                  final StateSerde<T> serde,
                                  final boolean consumerEnabled) {
        KafkaBasedLogFactory factory = new KafkaBasedLogFactory(producerProps, consumerProps);
        this.kafkaLog = factory.make(topic, new ConsumeCallback());
        this.groupId = sanitizeGroupId(groupId);
        this.serde = serde;
        this.keyPrefix = keyPrefix;
        this.consumerEnabled = consumerEnabled;
    }

    private static String sanitizeGroupId(final String groupId) {
        return groupId.replaceAll("\\.", "-");
    }

    Status getState() {
        return this.status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start() {
        if (isStarted()) {
            throw new IllegalStateException("Cannot init again.");
        }
        LOG.info("Starting {}", getBackingStoreName());
        // Before startup, callbacks are *not* invoked. You can grab a snapshot after starting -- just take care that
        // updates can continue to occur in the background
        kafkaLog.start(consumerEnabled);
        this.status = Status.STARTED;
        LOG.info("Started {}", getBackingStoreName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStarted() {
        return getState().equals(Status.STARTED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        synchronized (this) {
            LOG.info("Closing {}", getBackingStoreName());
            this.status = Status.PENDING_SHUTDOWN;
            kafkaLog.flush();
            kafkaLog.stop();
            this.status = Status.SHUTDOWN;
            LOG.info("Closed {}", getBackingStoreName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StateSnapshot<T> snapshot() {
        synchronized (lock) {
            return new StateSnapshot<>(offset.get(), Collections.unmodifiableMap(states));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(final String name) {
        synchronized (lock) {
            return states.containsKey(name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAsync(final String name, final T state) {
        put(name, state, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final String name, final T state) {
        put(name, state, true);
    }

    private void put(final String name, final T state, final boolean sync) {
        checkStates();
        try {
            safeSend(name, serde.serialize(state));
        } catch (final Exception e) {
            LOG.error("Failed to write state to Kafka: ", e);
            throw new RuntimeException("Error writing state to Kafka", e);
        }
        mayRefreshState(sync);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAsync(final String name) {
        remove(name, false);
    }

    private void remove(final String name, final boolean sync) {
        checkStates();
        LOG.debug("Removing state for name {}", name);
        try {
            safeSend(name, null);
        } catch (final Exception e) {
            LOG.error("Failed to remove state from Kafka: ", e);
            throw new RuntimeException("Error removing state from Kafka", e);
        }
        mayRefreshState(sync);
    }

    private void mayRefreshState(final boolean refresh) {
        try {
            if (refresh) {
                refresh(DEFAULT_READ_TO_END_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (final TimeoutException e) {
            LOG.error("Failed to synchronize state from Kafka: TimeoutException");
        }
    }

    private void safeSend(final String key, final byte[] value) {
        kafkaLog.send(newRecordKey(groupId, key), value, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                if (exception == null) return;
                if (exception instanceof RetriableException) {
                    if (value == null) {
                        kafkaLog.send(key, null, this);
                    }
                } else {
                    LOG.error("Failed to write state update", exception);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(final String name) {
        remove(name, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void refresh(final long timeout, final TimeUnit unit) throws TimeoutException {
        checkStates();

        if (!consumerEnabled) {
            LOG.warn("This KafkaStateBackingStore is running in producer mode only. Refresh is ignored.");
            return;
        }

        try {
            kafkaLog.readToEnd().get(timeout, unit);
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error trying to read to end of log", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setUpdateListener(final StateBackingStore.UpdateListener<T> listener) {
        this.updateListener = listener;
    }

    private String getBackingStoreName() {
        return this.getClass().getSimpleName();
    }

    private synchronized void checkStates() {
        if (status == Status.SHUTDOWN ||
            status == Status.PENDING_SHUTDOWN) {
            throw new IllegalStateException("Bad state " + getState().name());
        }
    }

    private String newRecordKey(final String groupId, final String stateName) {
        return keyPrefix + groupId + GROUP_STATE_SEPARATOR + stateName;
    }

    public enum Status {
        CREATED, STARTED, PENDING_SHUTDOWN, SHUTDOWN
    }

    public class ConsumeCallback implements Callback<ConsumerRecord<String, byte[]>> {

        @Override
        public void onCompletion(Throwable error, ConsumerRecord<String, byte[]> record) {
            if (error != null) {
                LOG.error("Unexpected in consumer callback for KafkaStateBackingStore: ", error);
                return;
            }

            offset.set(record.offset() + 1);

            final byte[] value = record.value();
            final String key = record.key();

            if (key != null && key.startsWith(keyPrefix)) {
                String[] groupAndState = key.substring(keyPrefix.length()).split("\\.", 2);
                String recordGroup = groupAndState[0];
                String stateName = groupAndState[1];
                if (recordGroup.equals(groupId)) {
                    boolean removed = false;
                    T newState = null;
                    synchronized (lock) {
                        if (value == null) {
                            // Connector deletion will be written as a null value
                            LOG.debug(
                                    "Removed state {} due to null configuration. This is usually intentional and does not indicate an issue.",
                                    stateName);
                            states.remove(stateName);
                            removed = true;
                        } else {
                            try {
                                newState = serde.deserialize(value);
                            } catch (Exception e) {
                                LOG.error("Failed to read state : {}", stateName, e);
                                return;
                            }
                            LOG.debug("Updating state for name {} : {}", stateName, newState);
                            states.put(stateName, newState);
                        }
                    }

                    if (status == Status.STARTED && updateListener != null) {
                        if (removed) {
                            updateListener.onStateRemove(stateName);
                        } else {
                            updateListener.onStateUpdate(stateName, newState);
                        }
                    }
                } else {
                    LOG.trace("Discarding state update value - not belong to group {} : {}", groupId,
                            key);
                }
            } else {
                LOG.warn("Discarding state update value with invalid key : {}", key);
            }
        }
    }
}