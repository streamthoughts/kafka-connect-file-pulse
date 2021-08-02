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
package io.streamthoughts.kafka.connect.filepulse.source;

import io.streamthoughts.kafka.connect.filepulse.config.TaskConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.filter.DefaultRecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.fs.FileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.state.FileObjectStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.state.FileObjectStateBackingStoreManager;
import io.streamthoughts.kafka.connect.filepulse.state.internal.OpaqueMemoryResource;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG;

/**
 * The FilePulseSourceTask.
 */
public class FilePulseSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseSourceTask.class);

    private static final int CONSECUTIVE_WAITS_BEFORE_RETURN = 3;

    private static final String CONNECT_NAME_CONFIG = "name";

    private static final Integer NO_PARTITION = null;

    private static final int DEFAULT_POLL_WAIT_MS = 500;

    public TaskConfig taskConfig;

    private String topic;

    private DefaultFileRecordsPollingConsumer consumer;

    private SourceOffsetPolicy offsetPolicy;

    private KafkaFileStateReporter reporter;

    private volatile FileContext contextToBeCommitted;

    private OpaqueMemoryResource<StateBackingStore<FileObject>> sharedStore;

    private FileURIProvider fileURIProvider;

    private String connectorGroupName;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * {@inheritDoc}
     */
    @Override
    public String version() {
        return new FilePulseSourceConnector().version();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(final Map<String, String> props) {
        LOG.info("Starting FilePulse source task");

        final Map<String, String> configProperties = new HashMap<>(props);
        // Disable backing store Kafka Consumer if KafkaFileObjectStateBackingStore is configured.
        configProperties.put(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG, "false");

        taskConfig = new TaskConfig(configProperties);
        connectorGroupName = props.get(CONNECT_NAME_CONFIG);
        offsetPolicy = taskConfig.getSourceOffsetPolicy();
        topic = taskConfig.topic();

        try {
            initSharedStateBackingStore(connectorGroupName);

            reporter = new KafkaFileStateReporter(sharedStore.getResource());
            consumer = newFileRecordsPollingConsumer();
            consumer.setFileListener(reporter);
            fileURIProvider = taskConfig.getFileURIProvider();

            running.set(true);
            LOG.info("Started FilePulse source task");
        } catch (final Throwable t) {
            // This task has failed, so close any resources (may be reopened if needed) before throwing
            closeResources();
            throw t;
        }
    }

    private DefaultFileRecordsPollingConsumer newFileRecordsPollingConsumer() {
        final RecordFilterPipeline<FileRecord<TypedStruct>> filter = new DefaultRecordFilterPipeline(
                taskConfig.filters()
        );
        return new DefaultFileRecordsPollingConsumer(
                context,
                taskConfig.reader(),
                filter,
                offsetPolicy,
                taskConfig.isReadCommittedFile());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LOG.trace("Polling for new data");
        try {
            final MaxConsecutiveAttempts consecutiveWaits = new MaxConsecutiveAttempts(CONSECUTIVE_WAITS_BEFORE_RETURN);

            contextToBeCommitted = consumer.context();

            while (running.get()) {
                if (!consumer.hasNext()) {
                    contextToBeCommitted = null;
                    if (fileURIProvider.hasMore()) {
                        consumer.addAll(fileURIProvider.nextURIs());
                        // fileURIProvider may have more URIs but still return empty collection
                        // if files are not immediately available. In this case, this method should
                        // be blocked before returning.
                        if (!consumer.hasNext() && consecutiveWaits.checkAndDecrement()) {
                            // Check if the SourceTask is still running to
                            // return immediately instead of waiting
                            if (!running.get()) continue;
                            busyWait();
                        }
                    } else {
                        LOG.info(
                            "Completed all object files. FilePulse source task is transitioning to " +
                            "IDLE state while waiting for new reconfiguration request from source connector."
                        );
                        // we can safely close resources for this task
                        this.running.set(false);
                        closeResources();
                        synchronized (this) {
                            // Wait for the source task being stopped by the TaskWorker
                            this.wait();
                        }
                        return null; // return directly as resources are already closed
                    }
                } else {
                    List<SourceRecord> results = null;
                    final RecordsIterable<FileRecord<TypedStruct>> records = consumer.next();

                    if (!records.isEmpty()) {
                        final FileContext context = consumer.context();
                        LOG.debug("Returning {} records for {}", records.size(), context.metadata());
                        results = records.stream().map(r -> buildSourceRecord(context, r)).collect(Collectors.toList());

                    } else if (consumer.hasNext() && consecutiveWaits.checkAndDecrement()) {
                        // Check if the SourceTask is still running to
                        // return immediately instead of waiting
                        if (!running.get()) continue;
                        busyWait();
                        continue;
                    }

                    // Check if the SourceTask should stop to close resources.
                    if (!running.get()) continue;

                    return results;
                }
            }
        } catch (final Throwable t) {
            // This task has failed, so close any resources (may be reopened if needed) before throwing
            closeResources();
            throw t;
        }
        // Only in case of shutdown
        closeResources();
        LOG.info("Stopped FilePulse source task.");
        return null;
    }

    private void busyWait() throws InterruptedException {
        LOG.trace("Waiting {} ms to poll next records", DEFAULT_POLL_WAIT_MS);
        Thread.sleep(DEFAULT_POLL_WAIT_MS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        if (running.get() && contextToBeCommitted != null) {
            reporter.notify(
                contextToBeCommitted.key(),
                contextToBeCommitted.metadata(),
                contextToBeCommitted.offset(),
                FileObjectStatus.READING
            );
        }
    }

    private SourceRecord buildSourceRecord(final FileContext context,
                                           final FileRecord<?> record) {
        final FileObjectMeta metadata = context.metadata();

        final Map<String, ?> sourcePartition = offsetPolicy.toPartitionMap(metadata);
        final Map<String, ?> sourceOffsets = offsetPolicy.toOffsetMap(record.offset().toSourceOffset());

        return record.toSourceRecord(
            sourcePartition,
            sourceOffsets,
            context.metadata(),
            topic,
            NO_PARTITION
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOG.info("Stopping FilePulse source task");
        running.set(false);
        synchronized (this) {
            notify();
        }
    }

    private void closeResources() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("Closing resources FilePulse source task");
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (Throwable t) {
                LOG.warn("Error while closing task", t);
            } finally {
                contextToBeCommitted = null;
                consumer = null;
                closeSharedStateBackingStore();
                LOG.info("Closed resources FilePulse source task");
            }
        }
    }

    private void initSharedStateBackingStore(final String connectorGroupName) {
        try {
            LOG.info("Retrieving access to shared backing store");
            sharedStore = FileObjectStateBackingStoreManager.INSTANCE
                    .getOrCreateSharedStore(
                            connectorGroupName,
                            () -> {
                                final FileObjectStateBackingStore store = taskConfig.getStateBackingStore();
                                // Always invoke the start() method when store is created from Task
                                // because this means the connector is running on a remote worker.
                                store.start();
                                return store;
                            },
                            new Object()
                    );
        } catch (Exception exception) {
            throw new ConnectException(
                    "Failed to create shared StateBackingStore for group '" + connectorGroupName + "'.",
                    exception
            );
        }
    }

    private void closeSharedStateBackingStore() {
        try {
            if (sharedStore != null) {
                sharedStore.close();
            }
        } catch (Exception exception) {
            LOG.error("Failed to shared StateBackingStore '{}'", connectorGroupName);
        }
    }

    private static final class MaxConsecutiveAttempts {

        final AtomicInteger consecutiveAttempts;

        private MaxConsecutiveAttempts(final int maxConsecutiveAttempts) {
            this.consecutiveAttempts = new AtomicInteger(maxConsecutiveAttempts);
        }

        public boolean checkAndDecrement() {
            if (getRemaining() < 0) {
                throw new IllegalStateException("cannot make a new consecutive attempt (remaining=0)");
            }
            return this.consecutiveAttempts.getAndDecrement() > 0;
        }

        private int getRemaining() {
            return consecutiveAttempts.get();
        }
    }
}