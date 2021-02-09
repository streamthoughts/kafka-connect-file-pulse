/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.clean.BatchFileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.clean.DelegateBatchFileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicyResult;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicyResultSet;
import io.streamthoughts.kafka.connect.filepulse.clean.GenericFileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.internal.KeyValuePair;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
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
 *  A default {@link FileSystemScanner} that can be used to trigger file
 */
public class DefaultFileSystemScanner implements FileSystemScanner {

    private enum ScanStatus {
        CREATED, READY, STARTED, STOPPED
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultFileSystemScanner.class);

    private static final long READ_CONFIG_ON_START_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

    private final static Comparator<FileObjectMeta> BY_LAST_MODIFIED =
        Comparator.comparingLong(FileObjectMeta::lastModified);

    private final FileSystemListing fsListing;

    private final StateBackingStore<FileObject> store;

    // List of files to be scheduled or currently being processed by tasks.
    private final Map<String, FileObjectMeta> scheduled = new ConcurrentHashMap<>();

    // List of files for which an event of completion has been received - those files are waiting for cleanup
    private final LinkedBlockingQueue<FileObject> completed = new LinkedBlockingQueue<>();

    private StateSnapshot<FileObject> fileState;

    private final SourceOffsetPolicy offsetPolicy;

    private final BatchFileCleanupPolicy cleaner;

    private final Long allowTasksReconfigurationAfterTimeoutMs;

    private Long nextAllowedTasksReconfiguration = -1L;

    private ScanStatus status;

    /**
     * Creates a new {@link DefaultFileSystemScanner} instance.
     *
     * @param allowTasksReconfigurationAfterTimeoutMs   {@code true} to allow tasks reconfiguration after a timeout.
     * @param fsListening                               the {@link FileSystemListing} to be used for listing object files.
     * @param cleaner                                   the {@link GenericFileCleanupPolicy} to be used for cleaning object files.
     * @param offsetPolicy                              the {@link SourceOffsetPolicy} to be used computing offset for object fileS.
     * @param store                                     the {@link StateBackingStore} used for storing object file cursor.
     */
    public DefaultFileSystemScanner(final Long allowTasksReconfigurationAfterTimeoutMs,
                                    final FileSystemListing fsListening,
                                    final GenericFileCleanupPolicy cleaner,
                                    final SourceOffsetPolicy offsetPolicy,
                                    final StateBackingStore<FileObject> store) {
        Objects.requireNonNull(fsListening, "fsListening can't be null");
        Objects.requireNonNull(cleaner, "cleaner can't be null");

        this.fsListing = fsListening;
        this.allowTasksReconfigurationAfterTimeoutMs = allowTasksReconfigurationAfterTimeoutMs;

        if (cleaner instanceof FileCleanupPolicy) {
            this.cleaner = new DelegateBatchFileCleanupPolicy((FileCleanupPolicy)cleaner);
        } else if (cleaner instanceof BatchFileCleanupPolicy) {
            this.cleaner = (BatchFileCleanupPolicy) cleaner;
        } else {
            throw new IllegalArgumentException("Cleaner must be one of 'FileCleanupPolicy', "
                    + "'BatchFileCleanupPolicy'" + " not " + cleaner.getClass().getName());
        }
        this.offsetPolicy = offsetPolicy;
        this.store = store;
        this.status = ScanStatus.CREATED;
        LOG.info("Creating local filesystem scanner");
        // The listener is not call until the store is fully STARTED.
        this.store.setUpdateListener(new StateBackingStore.UpdateListener<FileObject>() {
            @Override
            public void onStateRemove(final String key) {
                /* ignore */
            }

            @Override
            public void onStateUpdate(final String key, final FileObject state) {
                final FileObjectStatus status = state.status();
                if (status.isOneOf(FileObjectStatus.completed())) {
                    completed.add(state);
                } else if (status.isOneOf(FileObjectStatus.CLEANED)) {
                    final String partition = offsetPolicy.toPartitionJson(state.metadata());
                    final FileObjectMeta remove = scheduled.remove(partition);
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
                .filter(s -> s.status().isOneOf(FileObjectStatus.completed()))
                .forEach(completed::add);
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
            final List<FileObject> cleanable = new ArrayList<>(completed.size());
            completed.drainTo(cleanable);

            FileCleanupPolicyResultSet cleaned = cleaner.apply(cleanable);
            cleaned.forEach( (source, result) -> {
                if (result.equals(FileCleanupPolicyResult.SUCCEED)) {
                    final String partition = offsetPolicy.toPartitionJson(source.metadata());
                    store.put(partition, source.withStatus(FileObjectStatus.CLEANED));
                } else {
                    LOG.info("Postpone clean up for file '{}'", source);
                    completed.add(source);
                }
            });
            LOG.info("Finished cleaning all completed source files");
        }
    }

    private synchronized boolean updateFiles() {
        final boolean noScheduledFiles = scheduled.isEmpty();
        if (!noScheduledFiles && allowTasksReconfigurationAfterTimeoutMs == Long.MAX_VALUE) {
            LOG.info(
                "Scheduled files still being processed: {}. Skip directory scan while waiting for tasks completion",
                scheduled.size()
            );
            return false;
        }

        boolean toEnd = readStatesToEnd(TimeUnit.SECONDS.toMillis(5));
        if (noScheduledFiles && !toEnd) {
            LOG.warn("Finished scanning directory. Skip task reconfiguration due to timeout");
            return false;
        }

        LOG.info("Scanning local file system directory");
        final Collection<FileObjectMeta> objects = fsListing.listObjects();
        LOG.info("Completed scanned, number of source objects found '{}' ", objects.size());

        final StateSnapshot<FileObject> snapshot = store.snapshot();
        final Map<String, FileObjectMeta> toScheduled = toScheduled(objects, snapshot);

        // Some scheduled files are still being processed, but new files are detected
        if (!noScheduledFiles) {
            if (scheduled.keySet().containsAll(toScheduled.keySet())) {
                LOG.info(
                    "Scheduled files still being processed ({}) and no new files found. Skip task reconfiguration",
                    scheduled.size()
                );
                return false;
            }
            if (nextAllowedTasksReconfiguration == -1) {
                nextAllowedTasksReconfiguration = Time.SYSTEM.milliseconds() + allowTasksReconfigurationAfterTimeoutMs;
            }

            long timeout = Math.max(0, nextAllowedTasksReconfiguration - Time.SYSTEM.milliseconds());
            if (timeout > 0) {
                LOG.info(
                    "Scheduled files still being processed ({}) but new files detected. " +
                    "Waiting for {} ms before allowing tasks reconfiguration",
                    scheduled.size(),
                    timeout
                );
                return false;
            }
        }

        nextAllowedTasksReconfiguration = -1L;
        scheduled.putAll(toScheduled);

        notifyAll();

        LOG.info("Finished lookup for new files : '{}' files can be scheduled for processing", scheduled.size());
        // Only return true if the status is started, i.e. if this was not the first directory scan
        // This is used to not trigger task reconfiguration before the connector is fully started.
        return !scheduled.isEmpty() && status.equals(ScanStatus.STARTED);
    }

    private Map<String, FileObjectMeta> toScheduled(final Collection<FileObjectMeta> scanned,
                                                    final StateSnapshot<FileObject> snapshot) {

        final List<KeyValuePair<String, FileObjectMeta>> toScheduled = scanned.stream()
            .map(source -> KeyValuePair.of(offsetPolicy.toPartitionJson(source), source))
            .filter(kv -> maybeScheduled(snapshot, kv.key))
            .collect(Collectors.toList());

        // Looking for duplicates in sources files, i.e the OffsetPolicy generate two identical offsets for two files.
        final Stream<Map.Entry<String, List<KeyValuePair<String, FileObjectMeta>>>> entryStream = toScheduled
            .stream()
            .collect(Collectors.groupingBy(kv -> kv.key))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 1);

        final Map<String, List<String>> duplicates = entryStream
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().stream().map(m -> m.value.stringURI()).collect(Collectors.toList()))
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

    private boolean maybeScheduled(final StateSnapshot<FileObject> snapshot, final String partition) {
        return !snapshot.contains(partition) || snapshot.getForKey(partition).status().isOneOf(FileObjectStatus.started());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized List<List<URI>> partitionFilesAndGet(int maxGroups) {
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

        List<List<URI>> partitions;

        if (scheduled.isEmpty()) {
            LOG.warn("Directory could not be scanned quickly enough, or no file detected after connector started");
            partitions =  Collections.emptyList();
        } else {

            int numGroups = Math.min(scheduled.size(), maxGroups);

            List<FileObjectMeta> sources = new ArrayList<>(scheduled.values());
            sources.sort(BY_LAST_MODIFIED);

            final List<URI> paths = sources
                .stream()
                .map(FileObjectMeta::uri)
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