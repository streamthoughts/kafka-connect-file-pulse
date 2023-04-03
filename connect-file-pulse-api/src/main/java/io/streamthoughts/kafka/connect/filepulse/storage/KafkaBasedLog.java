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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBasedLog<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBasedLog.class);
    private static final long CREATE_TOPIC_TIMEOUT_MS = 30000;

    private final Time time;
    private final String topic;
    private final Map<String, Object> producerConfigs;
    private final Map<String, Object> consumerConfigs;
    private final Callback<ConsumerRecord<K, V>> consumedCallback;
    private Consumer<K, V> consumer;
    private volatile Producer<K, V> producer;

    private Thread thread;
    private boolean stopRequested;
    private final Queue<Callback<Void>> readLogEndOffsetCallbacks;
    private final Runnable initializer;

    private volatile States state;

    enum States {
        CREATED, RUNNING, ERROR, PENDING_SHUTDOWN, CLOSED
    }

    /**
     * Create a new KafkaBasedLog object. This does not start reading the log and writing is not permitted until
     * {@link #start(boolean)} is invoked.
     *
     * @param topic the topic to treat as a log
     * @param producerConfigs configuration options to use when creating the internal producer. At a minimum this must
     *                        contain compatible serializer settings for the generic types used on this class. Some
     *                        setting, such as the number of acks, will be overridden to ensure correct behavior of this
     *                        class.
     * @param consumerConfigs configuration options to use when creating the internal consumer. At a minimum this must
     *                        contain compatible serializer settings for the generic types used on this class. Some
     *                        setting, such as the auto startPosition reset policy, will be overridden to ensure correct
     *                        behavior of this class.
     * @param consumedCallback callback to invoke for each {@link ConsumerRecord} consumed when tailing the log
     * @param time Time interface
     * @param initializer the component that should be run when this log is {@link #start(boolean) started}; may be null
     */
    KafkaBasedLog(final String topic,
                  final Map<String, Object> producerConfigs,
                  final Map<String, Object> consumerConfigs,
                  final Callback<ConsumerRecord<K, V>> consumedCallback,
                  final Time time,
                  final Runnable initializer) {
        this.topic = topic;
        this.producerConfigs = producerConfigs;
        this.consumerConfigs = consumerConfigs;
        this.consumedCallback = consumedCallback;
        this.stopRequested = false;
        this.readLogEndOffsetCallbacks = new ArrayDeque<>();
        this.time = time;
        this.initializer = initializer != null ? initializer : () -> { };
        this.state = States.CREATED;
    }

    public synchronized void start(final boolean consumerEnabled) {
        if (state != States.CREATED) {
            throw new IllegalStateException("Cannot restart KafkaBasedLog due to state being " + state +")");
        }
        LOG.info("Starting KafkaBasedLog with topic {} (consumer_mode_enabled={})",  topic, consumerEnabled);
        try {
            initializer.run();
            producer = createProducer();
            if (consumerEnabled) {
                consumer = createConsumer();

                List<TopicPartition> partitions = new ArrayList<>();

                List<PartitionInfo> partitionInfos = null;
                long started = time.milliseconds();
                while (partitionInfos == null && time.milliseconds() - started < CREATE_TOPIC_TIMEOUT_MS) {
                    partitionInfos = consumer.partitionsFor(topic);
                    Utils.sleep(Math.min(time.milliseconds() - started, 1000));
                }
                if (partitionInfos == null)
                    throw new ConnectException(
                        "Could not look up partition metadata for position backing store topic '" + topic + "' in" +
                        " allotted period (" + CREATE_TOPIC_TIMEOUT_MS + "ms). This could indicate a connectivity issue," +
                        " unavailable topic partitions, or if this is your first use of the topic it may have taken too long to create.");

                for (PartitionInfo partition : partitionInfos)
                    partitions.add(new TopicPartition(partition.topic(), partition.partition()));
                consumer.assign(partitions);

                // Always consume from the beginning of all partitions. Necessary to ensure that we don't use committed offsets
                // when a 'group.id' is specified (if offsets happen to have been committed unexpectedly).
                consumer.seekToBeginning(partitions);

                readToLogEnd();

                thread = new WorkThread();
                thread.start();
                LOG.info("Finished reading KafkaBasedLog for topic {}", topic);
            }
            state = States.RUNNING;
            LOG.info("Started KafkaBasedLog for topic {}", topic);
        } catch (Exception e) {
            state = States.ERROR;
            throw e;
        }
    }

    public void stop() {
        if (state == States.PENDING_SHUTDOWN ||
            state == States.CLOSED) {
            LOG.info("KafkaBasedLog is either being shutdown or already closed for topic {}", topic);
            return;
        }

        LOG.info("Stopping KafkaBasedLog for topic {}", topic);
        synchronized (this) {
            stopRequested = true;
        }
        try {
            if (consumer != null) consumer.wakeup();

            try {
                if (thread != null) thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException("Failed to stop KafkaBasedLog. Exiting without cleanly shutting " +
                        "down it's producer and consumer.", e);
            }

            try {
                if (producer != null) producer.close();
            } catch (KafkaException e) {
                LOG.error("Failed to stop KafkaBasedLog producer", e);
            }

            try {
                if (consumer != null) consumer.close();
            } catch (KafkaException e) {
                LOG.error("Failed to stop KafkaBasedLog consumer", e);
            }

            LOG.info("Stopped KafkaBasedLog for topic {}", topic);
        } finally {
            state = States.CLOSED;
        }
    }

    /**
     * Flushes any outstanding writes and then reads to the current end of the log and invokes the specified callback.
     * Note that this checks the current, offsets, reads to them, and invokes the callback regardless of whether
     * additional records have been written to the log. If the caller needs to ensure they have truly reached the end
     * of the log, they must ensure there are no other writers during this period.
     *
     * This waits until the end of all partitions has been reached.
     *
     * This method is asynchronous. If you need a synchronous version, pass an instance of
     * {@link FutureCallback} as the callback parameter and wait on it to block.
     *
     * @param callback the callback to invoke once the end of the log has been reached.
     */
    public void readToEnd(final Callback<Void> callback) {
        LOG.trace("Starting read to end log for topic {}", topic);
        producer.flush();
        synchronized (this) {
            readLogEndOffsetCallbacks.add(callback);
        }
        consumer.wakeup();
    }

    /**
     * Flush the underlying producer to ensure that all pending writes have been sent.
     */
    public void flush() {
        if (producer != null) {
            producer.flush();
        }
    }

    /**
     * Same as {@link #readToEnd(Callback)} but provides a {@link Future} instead of using a callback.
     * @return the future associated with the operation
     */
    public Future<Void> readToEnd() {
        FutureCallback<Void> future = new FutureCallback<>(null);
        readToEnd(future);
        return future;
    }

    public void send(final K key, final V value) {
        checkIsRunning();
        send(key, value, null);
    }

    public void send(final K key, final V value, final org.apache.kafka.clients.producer.Callback callback) {
        checkIsRunning();
        producer.send(new ProducerRecord<>(topic, key, value), callback);
    }

    private void checkIsRunning() {
        if (state != States.RUNNING) {
            throw new IllegalStateException("KafkaBasedLog is already not running.");
        }
    }

    private Producer<K, V> createProducer() {
        // Always require producer acks to all to ensure durable writes
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");

        // Don't allow more than one in-flight request to prevent reordering on retry (if enabled)
        producerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return new KafkaProducer<>(producerConfigs);
    }

    private Consumer<K, V> createConsumer() {
        // Always force reset to the beginning of the log since this class wants to consume all available log value
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Turn off autocommit since we always want to consume the full log
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(consumerConfigs);
    }

    private void poll(final Duration timeout) {
        try {
            ConsumerRecords<K, V> records = consumer.poll(timeout);
            for (ConsumerRecord<K, V> record : records)
                consumedCallback.onCompletion(null, record);
        } catch (WakeupException e) {
            // Expected on instance() or stop(). The calling code should handle this
            throw e;
        } catch (KafkaException e) {
            LOG.error("Error polling: " + e);
        }
    }

    private void readToLogEnd() {
        LOG.trace("Reading to end of startPosition log");

        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        LOG.trace("Reading to end of log offsets {}", endOffsets);

        while (!endOffsets.isEmpty()) {
            Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<TopicPartition, Long> entry = it.next();
                if (consumer.position(entry.getKey()) >= entry.getValue())
                    it.remove();
                else {
                    poll(Duration.ofMillis(Integer.MAX_VALUE));
                    break;
                }
            }
        }
    }


    private class WorkThread extends Thread {
        WorkThread() {
            super("KafkaBasedLog Work Thread - " + topic);
        }

        @Override
        public void run() {
            try {
                LOG.trace("{} started execution", this);
                while (true) {
                    int numCallbacks;
                    synchronized (KafkaBasedLog.this) {
                        if (stopRequested)
                            break;
                        numCallbacks = readLogEndOffsetCallbacks.size();
                    }

                    if (numCallbacks > 0) {
                        try {
                            readToLogEnd();
                            LOG.trace("Finished read to end log for topic {}", topic);
                        } catch (WakeupException e) {
                            // Either received another instance() call and need to retry reading to end of log or stop() was
                            // called. Both are handled by restarting this loop.
                            continue;
                        }
                    }

                    synchronized (KafkaBasedLog.this) {
                        // Only invoke exactly the number of callbacks we found before triggering the read to log end
                        // since it is possible for another write + readToEnd to sneak in the meantime
                        for (int i = 0; i < numCallbacks; i++) {
                            Callback<Void> cb = readLogEndOffsetCallbacks.poll();
                            cb.onCompletion(null, null);
                        }
                    }

                    try {
                        poll(Duration.ofMillis(Integer.MAX_VALUE));
                    } catch (WakeupException e) {
                        // See previous comment, both possible causes of this wakeup are handled by starting this loop again
                        continue;
                    }
                }
            } catch (Throwable t) {
                LOG.error("Unexpected error in {}", this, t);
            }
        }
    }
}