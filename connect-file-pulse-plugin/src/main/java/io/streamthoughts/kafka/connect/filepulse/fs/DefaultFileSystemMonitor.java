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
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectKey;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default {@link FileSystemMonitor} that can be used to trigger file
 */
public class DefaultFileSystemMonitor implements FileSystemMonitor {

    private static final long TASK_CONFIGURATION_DEFAULT_TIMEOUT = 15000L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultFileSystemMonitor.class);

    private static final Duration ON_START_READ_END_LOG_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_READ_END_LOG_TIMEOUT = Duration.ofSeconds(5);
    private static final int MAX_SCHEDULE_ATTEMPTS = 3;

    private final FileSystemListing<?> fsListing;

    private final StateBackingStore<FileObject> store;

    // List of files currently being processed by tasks.
    private final Map<FileObjectKey, FileObjectMeta> scheduled = new ConcurrentHashMap<>();

    // List of files to be scheduled.
    private final Map<FileObjectKey, FileObjectMeta> scanned = new ConcurrentHashMap<>();

    // List of files that are cleanable.
    private final LinkedBlockingQueue<FileObject> cleanable = new LinkedBlockingQueue<>();

    private StateSnapshot<FileObject> fileState;

    private final SourceOffsetPolicy offsetPolicy;

    private final BatchFileCleanupPolicy cleaner;

    private final Long allowTasksReconfigurationAfterTimeoutMs;

    private Long nextAllowedTasksReconfiguration = -1L;

    private final AtomicBoolean taskReconfigurationRequested = new AtomicBoolean(false);

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicBoolean changed = new AtomicBoolean(false);

    private final AtomicBoolean fileSystemListingEnabled = new AtomicBoolean(true);

    private final Predicate<FileObjectStatus> cleanablePredicate;

    private final TaskFileOrder taskFileOrder;

    /**
     * Creates a new {@link DefaultFileSystemMonitor} instance.
     *
     * @param allowTasksReconfigurationAfterTimeoutMs {@code true} to allow tasks reconfiguration after a timeout.
     * @param fsListening                             the {@link FileSystemListing} to be used for listing object files.
     * @param cleanPolicy                                 the {@link GenericFileCleanupPolicy} to be used for cleaning object files.
     * @param offsetPolicy                            the {@link SourceOffsetPolicy} to be used computing offset for object fileS.
     * @param store                                   the {@link StateBackingStore} used for storing object file cursor.
     */
    public DefaultFileSystemMonitor(final Long allowTasksReconfigurationAfterTimeoutMs,
                                    final FileSystemListing<?> fsListening,
                                    final GenericFileCleanupPolicy<?, ?> cleanPolicy,
                                    final Predicate<FileObjectStatus> cleanablePredicate,
                                    final SourceOffsetPolicy offsetPolicy,
                                    final StateBackingStore<FileObject> store,
                                    final TaskFileOrder taskFileOrder) {
        Objects.requireNonNull(fsListening, "'fsListening' should not be null");
        Objects.requireNonNull(cleanPolicy, "'cleanPolicy' should not be null");
        Objects.requireNonNull(offsetPolicy, "'offsetPolicy' should not be null");
        Objects.requireNonNull(store, "'store' should not null");
        Objects.requireNonNull(cleanablePredicate, "'cleanablePredicate' should not null");
        Objects.requireNonNull(taskFileOrder, "'taskFileOrder' should not null");

        this.fsListing = fsListening;
        this.allowTasksReconfigurationAfterTimeoutMs = allowTasksReconfigurationAfterTimeoutMs;
        this.cleanablePredicate = cleanablePredicate;
        this.taskFileOrder = taskFileOrder;

        if (cleanPolicy instanceof FileCleanupPolicy) {
            this.cleaner = new DelegateBatchFileCleanupPolicy((FileCleanupPolicy) cleanPolicy);
        } else if (cleanPolicy instanceof BatchFileCleanupPolicy) {
            this.cleaner = (BatchFileCleanupPolicy) cleanPolicy;
        } else {
            throw new IllegalArgumentException("Cleaner must be one of 'FileCleanupPolicy', "
                    + "'BatchFileCleanupPolicy'" + " not " + cleanPolicy.getClass().getName());
        }
        this.cleaner.setStorage(fsListening.storage());
        this.offsetPolicy = offsetPolicy;
        this.store = store;
        LOG.info("Initializing FileSystemMonitor");
        // The listener is not call until the store is fully STARTED.
        this.store.setUpdateListener(new StateBackingStore.UpdateListener<>() {
            @Override
            public void onStateRemove(final String key) {
                /* ignore */
            }

            @Override
            public void onStateUpdate(final String key, final FileObject object) {
                final FileObjectKey objectId = FileObjectKey.of(key);
                final FileObjectStatus status = object.status();
                LOG.debug("Received status '{} 'for: {}", status, object);
                if (cleanablePredicate.test(status)) {
                    cleanable.add(object.withKey(objectId));
                    // We should always try to remove the object key from the list
                    // of last scanned files to avoid scheduling an object file that has just been completed.
                    if (scanned.remove(objectId) != null) {
                        changed.set(true);
                    }
                } else if (status.isOneOf(FileObjectStatus.CLEANED, FileObjectStatus.INVALID)) {
                    final FileObjectMeta removed = scheduled.remove(objectId);
                    if (removed == null && status.isOneOf(FileObjectStatus.CLEANED)) {
                        LOG.debug(
                            "Received cleaned status but no object-file currently scheduled for: '{}'. " +
                            "This warn should only occurred during recovering step",
                            key
                        );
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
        readStatesToEnd(ON_START_READ_END_LOG_TIMEOUT);
        recoverPreviouslyCompletedSources();
        // Trigger a cleanup during initialization to ensure that all cleanable
        // object-files are eventually removed before scheduling any tasks.
        cleanUpCompletedFiles();
        LOG.info("Initialized FileSystemMonitor");
    }

    private void recoverPreviouslyCompletedSources() {
        LOG.info("Recovering completed files from a previous execution");
        fileState.states()
                .entrySet()
                .stream()
                .map(it -> it.getValue().withKey(FileObjectKey.of(it.getKey())))
                .filter(it -> cleanablePredicate.test(it.status()))
                .forEach(cleanable::add);
        LOG.info("Finished recovering previously completed files : {}", cleanable);
    }

    private boolean readStatesToEnd(final Duration timeout) {
        try {
            store.refresh(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fileState = store.snapshot();
            LOG.debug(
                    "Finished reading to end of log and updated states snapshot, new states log position: {}",
                    fileState.offset());
            return true;
        } catch (TimeoutException e) {
            LOG.warn("Failed to reach end of states log quickly enough", e);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invoke(final ConnectorContext context) {
        // It seems to be OK to always run cleanup even if connector is not yet started or is being shut down.
        cleanUpCompletedFiles();
        if (running.get() && fileSystemListingEnabled.get()) {
            if (!taskReconfigurationRequested.get()) {
                if (updateFiles()) {
                    LOG.info("Requesting task reconfiguration");
                    taskReconfigurationRequested.set(true);
                    context.requestTaskReconfiguration();
                }
            } else {
                LOG.info("Task reconfiguration requested. Skip filesystem listing.");
            }
        } else if (fileSystemListingEnabled.get()) {
            LOG.info("The connector is not completely started or is being shut down. Skip filesystem listing.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFileSystemListingEnabled(final boolean enabled) {
        this.fileSystemListingEnabled.set(enabled);
    }

    private void cleanUpCompletedFiles() {
        if (cleanable.isEmpty()) {
            LOG.debug("Skipped cleanup. No object file completed.");
            return;
        }

        LOG.info("Cleaning up completed object files '{}'", cleanable.size());
        final List<FileObject> cleanable = new ArrayList<>(this.cleanable.size());
        this.cleanable.drainTo(cleanable);

        FileCleanupPolicyResultSet cleaned = cleaner.apply(cleanable);
        cleaned.forEach((fileObject, result) -> {
            if (result.equals(FileCleanupPolicyResult.SUCCEED)) {
                final String key = fileObject.key().get().original();
                store.putAsync(key, fileObject.withStatus(FileObjectStatus.CLEANED));
            } else {
                LOG.warn("Postpone clean up for object file: '{}'", fileObject.metadata().stringURI());
                this.cleanable.add(fileObject);
            }
        });
        LOG.info("Finished cleaning all completed object files");
    }

    private synchronized boolean updateFiles() {
        final boolean noScheduledFiles = scheduled.isEmpty();
        if (!noScheduledFiles && allowTasksReconfigurationAfterTimeoutMs == Long.MAX_VALUE) {
            LOG.info(
                "Scheduled files still being processed: {}. Skip filesystem listing while waiting for tasks completion",
                scheduled.size()
            );
            return false;
        }

        boolean toEnd = readStatesToEnd(DEFAULT_READ_END_LOG_TIMEOUT);
        if (noScheduledFiles && !toEnd) {
            LOG.warn("Failed to read state changelog. Skip filesystem listing due to timeout");
            return false;
        }

        LOG.info("Starting to list object files using: {}", fsListing.getClass().getSimpleName());
        long started = Time.SYSTEM.milliseconds();
        final Collection<FileObjectMeta> objects = fsListing.listObjects();
        long took = Time.SYSTEM.milliseconds() - started;
        LOG.info("Completed object files listing. '{}' object files found in {}ms", objects.size(), took);

        final StateSnapshot<FileObject> snapshot = store.snapshot();
        final Map<FileObjectKey, FileObjectMeta> toScheduled = FileObjectCandidatesFilter.filter(
                offsetPolicy,
                fileObjectKey -> {
                    final FileObject fileObject = snapshot.getForKey(fileObjectKey.original());
                    if (fileObject == null) return true;

                    final FileObjectStatus status = fileObject.status();
                    return !(cleanablePredicate.test(status) || status.isDone());
                },
                objects
        );

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
                        "Waiting for {} ms before allowing task reconfiguration",
                        scheduled.size(),
                        timeout
                );
                return false;
            }
        }

        nextAllowedTasksReconfiguration = -1L;
        scanned.putAll(toScheduled);

        notifyAll();

        LOG.info("Finished lookup for new object files: '{}' files can be scheduled for processing", scanned.size());
        // Only return true if the status is started, i.e. if this was not the first filesystem scan
        // This is used to not trigger task reconfiguration before the connector is fully started.
        return !scanned.isEmpty() && running.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FileObjectMeta> listFilesToSchedule(final int maxFilesToSchedule) {

        if (!running.get()) {
            // This is the first call of partitionFilesAndGet, hence the connector is starting or restarting after
            // a configuration update. An empty list must be returned to ensure that all running tasks is stopped
            // before scheduling new object files.
            LOG.info("Started FileSystemMonitor");
            running.set(true);
            return Collections.emptyList();
        }

        try {
            long started = Time.SYSTEM.milliseconds();
            long now = started;
            while (scanned.isEmpty() && now - started < TASK_CONFIGURATION_DEFAULT_TIMEOUT) {
                try {
                    synchronized (this) {
                        LOG.info("No file to be scheduled, waiting for next filesystem scan execution");
                        wait(Math.max(0, TASK_CONFIGURATION_DEFAULT_TIMEOUT - (now - started)));
                    }
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
                now = Time.SYSTEM.milliseconds();
            }

            List<FileObjectMeta> partitions = new LinkedList<>();

            // Re-check if there is still object files that may be scheduled.
            if (!scanned.isEmpty()) {
                int attempts = 0;
                do {
                    changed.set(false);
                    LOG.info(
                        "Preparing next scheduling using the object files found during last iteration (attempt={}/{}).",
                        attempts + 1,
                        MAX_SCHEDULE_ATTEMPTS
                    );
                    // Try to read states to end to make sure we do not attempt
                    // to schedule an object file that has been cleanup.
                    final boolean toEnd = readStatesToEnd(DEFAULT_READ_END_LOG_TIMEOUT);
                    if (!toEnd) {
                        LOG.warn("Failed to read state changelog while scheduling object files. Timeout.");
                    }

                    // Check if all scanned object-files can be schedule.
                    if (scanned.size() <= maxFilesToSchedule) {
                        scheduled.putAll(scanned);
                    } else {
                        final Iterator<Map.Entry<FileObjectKey, FileObjectMeta>> it = scanned.entrySet().iterator();
                        while (scheduled.size() < maxFilesToSchedule && it.hasNext()) {
                            final Map.Entry<FileObjectKey, FileObjectMeta> next = it.next();
                            scheduled.put(next.getKey(), next.getValue());
                        }
                    }

                    partitions = new ArrayList<>(scheduled.values());

                    attempts++;
                    if (changed.get()) {
                        if (attempts == MAX_SCHEDULE_ATTEMPTS) {
                            LOG.warn(
                                "Failed to prepare the object files after attempts: {}.",
                                MAX_SCHEDULE_ATTEMPTS
                            );
                            // Make sure to clear the schedule list before returning.
                            scheduled.clear();
                            return Collections.emptyList();
                        } else {
                            LOG.warn("State updates was received while preparing the object files to be scheduled");
                        }
                    }
                } while (changed.get() && attempts < MAX_SCHEDULE_ATTEMPTS);
            }

            if (partitions.isEmpty()) {
                LOG.warn(
                    "Filesystem could not be scanned quickly enough, " +
                    "or no object file was detected after starting the connector."
                );
            }
            return taskFileOrder.sort(partitions);
        } finally {
            scanned.clear();
            taskReconfigurationRequested.set(false);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            try {
                LOG.info("Closing FileSystemMonitor resources");
                readStatesToEnd(DEFAULT_READ_END_LOG_TIMEOUT);
                cleanUpCompletedFiles();
                LOG.info("Closed FileSystemMonitor resources");
            } catch (final Exception e) {
                LOG.warn("Unexpected error while closing FileSystemMonitor.", e);
            }
        }
    }
}