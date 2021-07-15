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
import java.util.stream.Collectors;

import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC;

/**
 * The FilePulseSourceTask.
 */
public class FilePulseSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseSourceTask.class);

    private static final String CONNECT_NAME_CONFIG = "name";

    private static final Integer NO_PARTITION = null;

    public TaskConfig taskConfig;

    private String topic;

    private DefaultFileRecordsPollingConsumer consumer;

    private SourceOffsetPolicy offsetPolicy;

    private KafkaFileStateReporter reporter;

    private volatile FileContext contextToBeCommitted;

    private OpaqueMemoryResource<StateBackingStore<FileObject>> sharedStore;

    private String connectorGroupName;

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
        LOG.info("Starting task");

        final Map<String, String> configProperties = new HashMap<>(props);
        configProperties.put(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC, "true");
        taskConfig = new TaskConfig(configProperties);

        connectorGroupName = props.get(CONNECT_NAME_CONFIG);
        initSharedStateBackingStore(connectorGroupName);

        offsetPolicy = taskConfig.getSourceOffsetPolicy();
        topic = taskConfig.topic();

        reporter = new KafkaFileStateReporter(sharedStore.getResource(), offsetPolicy);
        consumer = newFileRecordsPollingConsumer();
        consumer.setFileListener(reporter);
        consumer.addAll(taskConfig.files());
    }

    @SuppressWarnings("unchecked")
    private DefaultFileRecordsPollingConsumer newFileRecordsPollingConsumer() {
        final RecordFilterPipeline filter = new DefaultRecordFilterPipeline(taskConfig.filters());
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
        contextToBeCommitted = consumer.context();

        if (!consumer.hasNext()) {
            contextToBeCommitted = null;
            LOG.info("Orphan task detected - all scheduled files are now completed - waiting for new reconfiguration.");
            synchronized (this) {
                this.wait();
            }
            return null;
        }

        RecordsIterable<FileRecord<TypedStruct>> records = consumer.next();

        // If no records attempt to wait for incoming records.
        if (records.isEmpty() && consumer.hasNext()) {
            Thread.sleep(500);
            records = consumer.next();
        }

        FileContext context = consumer.context();
        if (records != null && !records.isEmpty()) {
            return records.stream().map(r -> buildSourceRecord(context, r)).collect(Collectors.toList());
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        if (contextToBeCommitted != null) {
            reporter.notify(
                contextToBeCommitted.metadata(),
                contextToBeCommitted.offset(),
                FileObjectStatus.READING);
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
        LOG.info("Stopping task.");
        synchronized (this) {
            if (consumer != null) {
                consumer.close();
                notify();
            }
            closeSharedStateBackingStore();
        }
        LOG.info("Task stopped.");
    }

    private void initSharedStateBackingStore(final String connectorGroupName) {
        try {
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
            sharedStore.close();
        } catch (Exception exception) {
            LOG.error("Failed to shared StateBackingStore '{}'", connectorGroupName);
        }
    }
}