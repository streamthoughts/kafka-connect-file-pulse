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
package io.streamthoughts.kafka.connect.filepulse.scanner;

import io.streamthoughts.kafka.connect.filepulse.clean.BatchFileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.clean.DelegateBatchFileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicyResult;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicyResultSet;
import io.streamthoughts.kafka.connect.filepulse.clean.GenericFileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.internal.KeyValuePair;
import io.streamthoughts.kafka.connect.filepulse.offset.OffsetManager;
import io.streamthoughts.kafka.connect.filepulse.scanner.local.FSDirectoryWalker;
import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.SourceStatus;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *  Default {@link FileSystemScanner} used to scan local file system.
 */
public class LocalFileSystemScanner implements FileSystemScanner {

    private enum ScanStatus {
        CREATED, READY, STARTED, STOPPED;
    }

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemScanner.class);

    private static final long READ_CONFIG_ON_START_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

    private final static Comparator<SourceMetadata> BY_LAST_MODIFIED =
        Comparator.comparingLong(SourceMetadata::lastModified);

    private final String sourceDirectoryPath;

    private final FSDirectoryWalker fsWalker;

    private final StateBackingStore<SourceFile> store;

    // List of files to be scheduled or currently being processed by tasks.
    private final Map<String, SourceMetadata> scheduled = new ConcurrentHashMap<>();

    // List of files for which an event of completion has been received - those files are waiting for cleanup
    private LinkedBlockingQueue<SourceFile> completed = new LinkedBlockingQueue<>();

    private StateSnapshot<SourceFile> fileState;

    private final OffsetManager offsetManager;

    private final BatchFileCleanupPolicy cleaner;

    private ScanStatus status;

    /**
     * Creates a new {@link LocalFileSystemScanner} instance.
     *
     * @param sourceDirectoryPath the source directory path to scan.
     * @param fsWalker            the walker used to scan FS directory.
     * @param cleaner             the file cleaner policy.
     * @param offsetManager       the offset manager.
     * @param store               the state store used to track file progression.
     */
    public LocalFileSystemScanner(final String sourceDirectoryPath,
                                  final FSDirectoryWalker fsWalker,
                                  final GenericFileCleanupPolicy cleaner,
                                  final OffsetManager offsetManager,
                                  final StateBackingStore<SourceFile> store) {
        Objects.requireNonNull(fsWalker, "fsWalker can't be null");
        Objects.requireNonNull(sourceDirectoryPath, "scanDirectoryPath can't be null");
        Objects.requireNonNull(cleaner, "cleaner can't be null");

        this.sourceDirectoryPath = sourceDirectoryPath;
        this.fsWalker = fsWalker;

        if (cleaner instanceof FileCleanupPolicy) {
            this.cleaner = new DelegateBatchFileCleanupPolicy((FileCleanupPolicy)cleaner);
        } else if (cleaner instanceof BatchFileCleanupPolicy) {
            this.cleaner = (BatchFileCleanupPolicy) cleaner;
        } else {
            throw new IllegalArgumentException("Cleaner must be one of 'FileCleanupPolicy', "
                    + "'BatchFileCleanupPolicy', or the variants that are consumer aware and/or "
                    + "Acknowledging"
                    + " not " + cleaner.getClass().getName());
        }
        this.offsetManager = offsetManager;
        this.store = store;
        this.status = ScanStatus.CREATED;
        LOG.info("Creating local filesystem scanner");
        // The listener is not call until the store is fully STARTED.
        this.store.setUpdateListener(new StateBackingStore.UpdateListener<SourceFile>() {
            @Override
            public void onStateRemove(final String key) {
                /* ignore */
            }

            @Override
            public void onStateUpdate(final String key, final SourceFile state) {
                final SourceStatus status = state.status();
                if (status.isOneOf(SourceStatus.completed())) {
                    completed.add(state);
                } else if (status.isOneOf(SourceStatus.CLEANED)) {
                    final String partition = offsetManager.toPartitionJson(state.metadata());
                    final SourceMetadata remove = scheduled.remove(partition);
                    if (remove == null) {
                        LOG.warn(
                            "Received cleaned status but no file currently scheduled for partition : '{}', " +
                            "this warn should only occurred during recovering step",
                            partition);
                    }
                }
            }
        });
        if (!this.store.isStarted()) {
            this.store.start();
        } else {
            LOG.warn("The StateBackingStore used to synchronize this connector " +
                "with tasks processing files is already started. You can ignore that warning if the connector " +
                " is recovering from a crash or resuming after being paused.");
        }
        readStatesToEnd(READ_CONFIG_ON_START_TIMEOUT_MS);
        recoverPreviouslyCompletedSources();
        this.status = ScanStatus.READY;
        LOG.info("Finished initializing local filesystem scanner");
    }

    private void recoverPreviouslyCompletedSources() {
        LOG.info("Recovering completed files from a previous execution");
        fileState.states().values()
                .stream()
                .filter(s -> s.status().isOneOf(SourceStatus.completed()))
                .forEach(s -> completed.add(s));
        LOG.info("Finished recovering previously completed files : " + completed);
    }

    private boolean readStatesToEnd(long timeoutMs) {
        try {
            store.refresh(timeoutMs, TimeUnit.MILLISECONDS);
            fileState = store.snapshot();
            LOG.debug(
                "Finished reading to end of log and updated states snapshot, new states log position: {}",
                fileState.offset());
            return true;
        } catch (TimeoutException e) {
            LOG.warn("Didn't reach end of states log quickly enough", e);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void scan(final ConnectorContext context) {
        cleanUpCompletedFiles();
        if (updateFiles()) {
            LOG.info("Requesting task reconfiguration");
            context.requestTaskReconfiguration();
        }
    }

    private void cleanUpCompletedFiles() {
        if (!completed.isEmpty()) {
            LOG.info("Cleaning up completed files '{}'", completed.size());
            final List<SourceFile> cleanable = new ArrayList<>(completed.size());
            completed.drainTo(cleanable);

            FileCleanupPolicyResultSet cleaned = cleaner.apply(cleanable);
            cleaned.forEach( (source, result) -> {
                if (result.equals(FileCleanupPolicyResult.SUCCEED)) {
                    final String partition = offsetManager.toPartitionJson(source.metadata());
                    store.put(partition, source.withStatus(SourceStatus.CLEANED));
                } else {
                    LOG.info("Postpone clean up for file '{}'", source);
                    completed.add(source);
                }
            });
            LOG.info("Finished cleaning all completed source files");
        }
    }

    private synchronized boolean updateFiles() {

        if (scheduled.isEmpty()) {
            LOG.info("Scanning local file system directory '{}'", sourceDirectoryPath);
            final Collection<File> files = fsWalker.listFiles(new File(sourceDirectoryPath));
            LOG.info("Completed scanned, number of files detected '{}' ", files.size());

            if (readStatesToEnd(TimeUnit.SECONDS.toMillis(5))) {
                final StateSnapshot<SourceFile> snapshot = store.snapshot();
                scheduled.putAll(toScheduled(files, snapshot));
                LOG.info("Finished lookup for new files : '{}' files selected", scheduled.size());

                notifyAll();
                // Only return true if the status is started, i.e. if this was not the first directory scan
                // This is used to not trigger task reconfiguration before the connector is fully started.
                return !scheduled.isEmpty() && status.equals(ScanStatus.STARTED);
            }
            LOG.info("Finished scanning directory '{}'", sourceDirectoryPath);
        } else {
            LOG.info(
                "Remaining in progress scheduled files: {}. Skip directory scan while waiting for tasks completion.",
                scheduled.size());
        }
        return false;
    }

    private Map<String, SourceMetadata> toScheduled(final Collection<File> scanned,
                                                    final StateSnapshot<SourceFile> snapshot) {

        final List<KeyValuePair<String, SourceMetadata>> toScheduled = scanned.stream()
            .map(SourceMetadata::fromFile)
            .map(metadata -> KeyValuePair.of(offsetManager.toPartitionJson(metadata), metadata))
            .filter(kv -> maybeScheduled(snapshot, kv.key))
            .collect(Collectors.toList());

        // Looking for duplicates in sources files, i.e the offsetManager generate two identical offsets for two files.
        final Stream<Map.Entry<String, List<KeyValuePair<String, SourceMetadata>>>> entryStream = toScheduled
            .stream()
            .collect(Collectors.groupingBy(kv -> kv.key))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 1);

        final Map<String, List<String>> duplicates = entryStream
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().stream().map(m -> m.value.absolutePath()).collect(Collectors.toList()))
            );

        if (!duplicates.isEmpty()) {
            final String strDuplicates = duplicates
                .entrySet()
                .stream()
                .map(e -> "offset=" + e.getKey() + ", files=" + e.getValue())
                .collect(Collectors.joining("\n\t", "\n\t","\n"));
            LOG.error("Duplicates detected in source files. Consider changing the configuration property \"offset.strategy\". Scan is ignored : {}", strDuplicates);
            return Collections.emptyMap(); // ignore all sources files
        }

        return toScheduled.stream().collect(Collectors.toMap(kv -> kv.key, kv -> kv.value));
    }

    private boolean maybeScheduled(final StateSnapshot<SourceFile> snapshot, final String partition) {
        return !snapshot.contains(partition) || snapshot.getForKey(partition).status().isOneOf(SourceStatus.started());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized List<List<String>> partitionFilesAndGet(int maxGroups) {
        LOG.info("Retrieving source files to be scheduled found during last scan");
        final long timeout = 15000L; // TODO: Timeout should be user-configurable
        long started = Time.SYSTEM.milliseconds();
        long now = started;
        while (scheduled.isEmpty() && now - started < timeout) {
            try {
                LOG.info("No file to be scheduled, waiting for next input directory scan execution");
                wait(timeout - (now - started));
            } catch (InterruptedException ignore) {
            }
            now = Time.SYSTEM.milliseconds();
        }

        List<List<String>> partitions;

        if (scheduled.isEmpty()) {
            LOG.warn("Directory could not be scanned quickly enough, or no file detected after connector started");
            partitions =  Collections.emptyList();
        } else {

            int numGroups = Math.min(scheduled.size(), maxGroups);

            List<SourceMetadata> sources = new ArrayList<>(scheduled.values());
            sources.sort(BY_LAST_MODIFIED);

            final List<String> paths = sources
                .stream()
                .map(SourceMetadata::absolutePath)
                .collect(Collectors.toList());
           partitions = ConnectorUtils.groupPartitions(paths, numGroups);
        }

        status = ScanStatus.STARTED;
        return partitions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        this.status = ScanStatus.STOPPED;
    }
}