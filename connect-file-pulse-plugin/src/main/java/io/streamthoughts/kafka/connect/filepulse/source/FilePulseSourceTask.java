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

import io.streamthoughts.kafka.connect.filepulse.config.SourceTaskConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.filter.DefaultRecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.fs.TaskFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.state.StateBackingStoreAccess;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The FilePulseSourceTask.
 */
public class FilePulseSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseSourceTask.class);

    private static final int CONSECUTIVE_WAITS_BEFORE_RETURN = 3;

    private static final String CONNECT_NAME_CONFIG = "name";

    public SourceTaskConfig taskConfig;

    private String defaultTopic;

    private DefaultFileRecordsPollingConsumer consumer;

    private SourceOffsetPolicy offsetPolicy;

    private FileObjectStateReporter reporter;

    private volatile FileObjectContext contextToBeCommitted;

    private StateBackingStoreAccess sharedStore;

    private TaskFileURIProvider fileURIProvider;

    private String connectorGroupName;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<FileObjectContext> completedToCommit = new ConcurrentLinkedQueue<>();

    private final Map<String, Schema> valueSchemas = new HashMap<>();

    private final AtomicLong taskThreadId = new AtomicLong(0);

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

        taskConfig = new SourceTaskConfig(configProperties);
        connectorGroupName = props.get(CONNECT_NAME_CONFIG);
        offsetPolicy = taskConfig.getSourceOffsetPolicy();
        defaultTopic = taskConfig.topic();
        valueSchemas.put(defaultTopic, taskConfig.getValueConnectSchema());
        try {
            sharedStore = new StateBackingStoreAccess(
                    connectorGroupName,
                    taskConfig::getStateBackingStore,
                    true
            );

            reporter = new FileObjectStateReporter(sharedStore.get().getResource()) {
                @Override
                public void onCompleted(final FileObjectContext context) {
                    super.onCompleted(context);
                    completedToCommit.add(context);
                }
            };

            consumer = newFileRecordsPollingConsumer();
            consumer.setStateListener(reporter);
            fileURIProvider = taskConfig.getFileURIProvider();

            running.set(true);
            taskThreadId.set(Thread.currentThread().getId());
            LOG.info("Started FilePulse source task");
        } catch (final Throwable t) {
            // This task has failed, so close any resources (maybe reopened if needed) before throwing
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
                List<SourceRecord> results = null;
                if (!consumer.hasNext()) {
                    contextToBeCommitted = null;
                    if (fileURIProvider.hasMore()) {
                        consumer.addAll(fileURIProvider.nextURIs());
                        // fileURIProvider may have more URIs but still return empty collection
                        // if no file is immediately available. In this case, this method should
                        // be blocked before returning.
                        if (!consumer.hasNext() &&
                                consecutiveWaits.checkAndDecrement()) {
                            // Check if the SourceTask is still running to
                            // return immediately instead of waiting
                            if (running.get()) busyWait();
                            continue;
                        }
                    } else {
                        LOG.info(
                                "Completed all object files. FilePulse source task is transitioning to " +
                                "IDLE state while waiting for new reconfiguration request from source connector."
                        );
                        running.set(false);

                        // we can safely close resources for this task if no more completed
                        // object files need to be committed
                        if (completedToCommit.isEmpty()) {
                            closeResources();
                        }

                        synchronized (this) {
                            // Wait for the source task being stopped by the TaskWorker
                            this.wait();
                        }

                        if (closed.get()) {
                            // Return directly as resources are already closed
                            return null;
                        }
                    }
                } else {

                    try {
                        final RecordsIterable<FileRecord<TypedStruct>> records = consumer.next();
                        if (!records.isEmpty()) {
                            final FileObjectContext context = consumer.context();
                            LOG.debug("Returning {} records for {}", records.size(), context.metadata());
                            results = records.stream()
                                    .map(r -> buildSourceRecord(context, r))
                                    .collect(Collectors.toList());

                            // Check if the SourceTask is still running to
                            // return immediately instead of waiting
                        } else if (running.get() &&
                                consumer.hasNext() &&
                                consecutiveWaits.checkAndDecrement()) {
                            busyWait();
                            continue;
                        }
                    } catch (ConnectFilePulseException e) {
                        if (taskConfig.isTaskHaltOnError()) {
                            throw e;
                        } else {
                            LOG.error("Caught unexpected error while processing file. Ignore and continue", e);
                        }
                    }
                }

                // Check if the SourceTask should stop to close resources.
                if (!running.get()) continue;
                return results;
            }
        } catch (final Throwable t) {
            // This task has failed, so close any resources (maybe reopened if needed) before throwing
            LOG.error("This task has failed due to uncaught error and will be stopped.");
            closeResources();
            throw t;
        }
        // Only in case of shutdown
        closeResources();
        LOG.info("Stopped FilePulse source task.");
        return null;
    }

    private void busyWait() throws InterruptedException {
        LOG.trace("Waiting {} ms to poll next records", taskConfig.getTaskEmptyPollWaitMs());
        Thread.sleep(taskConfig.getTaskEmptyPollWaitMs());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        if (running.get() && contextToBeCommitted != null) {
            reporter.notify(contextToBeCommitted, FileObjectStatus.READING);
        }

        if (!closed.get()) {
            while (!completedToCommit.isEmpty()) {
                final FileObjectContext file = completedToCommit.poll();
                LOG.info("Committed offset for file: {}", file.metadata());
                safelyCommit(file);
            }
        }
    }

    private void safelyCommit(final FileObjectContext committed) {
        try {
            reporter.notify(committed, FileObjectStatus.COMMITTED);
        } catch (Exception e) {
            LOG.warn("Failed to notify committed file: {}", context, e);
        }
    }

    private SourceRecord buildSourceRecord(final FileObjectContext context,
                                           final FileRecord<?> record) {
        final FileObjectMeta metadata = context.metadata();

        final Map<String, ?> sourcePartition = offsetPolicy.toPartitionMap(metadata);
        final Map<String, ?> sourceOffsets = offsetPolicy.toOffsetMap(record.offset().toSourceOffset());

        try {
            final SourceRecord result = record.toSourceRecord(
                    sourcePartition,
                    sourceOffsets,
                    context.metadata(),
                    defaultTopic,
                    null,
                    valueSchemas::get,
                    new FileRecord.ConnectSchemaMapperOptions(
                            taskConfig.isValueConnectSchemaMergeEnabled(),
                            taskConfig.isSchemaKeepLeadingUnderscoreOnFieldName()
                    )
            );

            if (taskConfig.isValueConnectSchemaMergeEnabled()) {
                valueSchemas.put(result.topic(), result.valueSchema());
            }

            return result;

        } catch (final Throwable t) {
            var exception = new ConnectFilePulseException(String.format(
                    "Failed to convert data into Kafka Connect record at offset %s from object-file: %s'",
                    context.offset(),
                    context.metadata()),
                    t
            );
            // Close internal iterator for the current object-file so that it will be marked as failed
            consumer.closeCurrentIterator(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOG.info("Stopping FilePulse source task");

        // In earlier versions of Kafka Connect, 'SourceTask::stop()' was not called from the task thread.
        // In this case, resources should be closed at the end of 'SourceTask::poll()'
        // when no longer running or if there is an error.
        running.set(false);

        // Since https://issues.apache.org/jira/browse/KAFKA-10792 the SourceTask::stop()
        // is called from the source task's dedicated thread
        if (taskThreadId.longValue() == Thread.currentThread().getId()) {
            closeResources();
            LOG.info("Stopped FilePulse source task.");
        } else {
            // For backward-compatibility with earlier versions of Kafka Connect.
            synchronized (this) {
                notify();
            }
        }
    }

    private void closeResources() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("Closing resources FilePulse source task");
            try {
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (final Throwable t) {
                        LOG.warn("Failed to close FileRecordsPollingConsumer. Error: {}", t.getMessage());
                    }
                }

                if (fileURIProvider != null) {
                    try {
                        fileURIProvider.close();
                    } catch (final Exception e) {
                        LOG.warn("Failed to close FileURIProvider. Error: {}", e.getMessage());
                    }
                }
            } finally {
                contextToBeCommitted = null;
                consumer = null;
                reporter = null;
                closeSharedStateBackingStore();
                LOG.info("Closed resources FilePulse source task");
            }
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

    static final class MaxConsecutiveAttempts {

        final AtomicInteger consecutiveAttempts;

        MaxConsecutiveAttempts(final int maxConsecutiveAttempts) {
            if (maxConsecutiveAttempts <= 0) {
                throw new IllegalArgumentException("'maxConsecutiveAttempts' must be superior to 0");
            }
            this.consecutiveAttempts = new AtomicInteger(maxConsecutiveAttempts);
        }

        public boolean checkAndDecrement() {
            if (getRemaining() < 0) {
                throw new IllegalStateException("cannot make a new consecutive attempt (remaining=0)");
            }
            return this.consecutiveAttempts.getAndDecrement() > 0;
        }

        int getRemaining() {
            return consecutiveAttempts.get();
        }
    }
}