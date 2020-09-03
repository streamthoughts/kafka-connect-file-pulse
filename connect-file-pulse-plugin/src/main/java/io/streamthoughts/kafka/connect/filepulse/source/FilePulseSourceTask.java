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
import io.streamthoughts.kafka.connect.filepulse.state.FileStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.state.StateBackingStoreRegistry;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.offset.OffsetManager;
import io.streamthoughts.kafka.connect.filepulse.offset.SimpleOffsetManager;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The FilePulseSourceTask.
 */
public class FilePulseSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseSourceTask.class);

    private static final Integer NO_PARTITION = null;

    public TaskConfig config;

    private String topic;

    private DefaultFileRecordsPollingConsumer consumer;

    private OffsetManager offsetManager;

    private StateBackingStore<SourceFile> store;

    private KafkaFileStateReporter reporter;

    private volatile FileContext contextToBeCommitted;

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
        config =  new TaskConfig(props);
        offsetManager = new SimpleOffsetManager(config.offsetStrategy());
        store = getStateStatesBackingStore();
        topic = config.topic();
        consumer = newFileRecordsPollingConsumer();
        reporter = new KafkaFileStateReporter(store, offsetManager);
        consumer.setFileListener(reporter);
        consumer.addAll(config.files());
    }

    @SuppressWarnings("unchecked")
    private DefaultFileRecordsPollingConsumer newFileRecordsPollingConsumer() {
        final RecordFilterPipeline filter = new DefaultRecordFilterPipeline(config.filters());
        return new DefaultFileRecordsPollingConsumer(
                context,
                config.reader(),
                filter,
                offsetManager,
                config.isReadCommittedFile());
    }

    private StateBackingStore<SourceFile> getStateStatesBackingStore() {
        final String groupId = config.getTasksReporterGroupId();
        StateBackingStoreRegistry.instance().register(groupId, () -> {
            final Map<String, Object> configs = config.getInternalKafkaReporterConfig();
            final String stateStoreTopic = config.getTaskReporterTopic();
            FileStateBackingStore store = new FileStateBackingStore(stateStoreTopic, groupId, configs, true);
            store.start();
            return store;
        });

        return StateBackingStoreRegistry.instance().get(groupId);
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
                SourceStatus.READING);
        }
    }

    private SourceRecord buildSourceRecord(final FileContext context,
                                           final FileRecord<?> record) {
        final SourceMetadata metadata = context.metadata();

        final Map<String, ?> sourcePartition = offsetManager.toPartitionMap(metadata);
        final Map<String, ?> sourceOffsets = offsetManager.toOffsetMap(record.offset().toSourceOffset());

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
            if (store != null) {
                StateBackingStoreRegistry.instance().release(config.getTasksReporterGroupId());
            }
        }
        LOG.info("Task stopped.");
    }
}