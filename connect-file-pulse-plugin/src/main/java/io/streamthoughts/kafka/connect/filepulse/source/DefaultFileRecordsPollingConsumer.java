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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is not thread-safe and is attended to be used only by one Source Connect Task.
 */
public class DefaultFileRecordsPollingConsumer implements FileRecordsPollingConsumer<FileRecord<TypedStruct>> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultFileRecordsPollingConsumer.class);

    private final Queue<DelegateFileInputIterator> queue;
    private final boolean ignoreCommittedOffsets;
    private final FileInputReader reader;
    private final RecordFilterPipeline<FileRecord<TypedStruct>> pipeline;
    private final SourceOffsetPolicy offsetPolicy;
    private StateListener listener;
    private final SourceTaskContext taskContext;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private FileRecord<TypedStruct> latestPolledRecord;

    private FileInputIterator<FileRecord<TypedStruct>> currentIterator;

    /**
     * Creates a new {@link DefaultFileRecordsPollingConsumer} instance.
     *
     * @param taskContext            the current task context.
     * @param reader                 the reader to be used.
     * @param pipeline               the filter pipeline to apply on each record.
     * @param offsetPolicy           the source offset/partition policy.
     * @param ignoreCommittedOffsets flag to indicate if committed offsets should be ignored.
     */
    DefaultFileRecordsPollingConsumer(final SourceTaskContext taskContext,
                                      final FileInputReader reader,
                                      final RecordFilterPipeline<FileRecord<TypedStruct>> pipeline,
                                      final SourceOffsetPolicy offsetPolicy,
                                      final boolean ignoreCommittedOffsets) {
        this.queue = new LinkedBlockingQueue<>();
        this.ignoreCommittedOffsets = ignoreCommittedOffsets;
        this.reader = reader;
        this.pipeline = pipeline;
        this.offsetPolicy = offsetPolicy;
        this.taskContext = taskContext;
    }

    void addAll(final List<URI> files) {
        if (isClosed()) {
            throw new IllegalStateException("Can't add new input files, consumer is closed");
        }
        final List<DelegateFileInputIterator> iterables = new ArrayList<>(files.size());
        for (final URI uri : files) {
            if (reader.canBeRead(uri)) {

                final FileObjectMeta objectMeta;
                final FileObjectKey key;
                try {
                    objectMeta = reader.getObjectMetadata(uri);
                    key = FileObjectKey.of(offsetPolicy.toPartitionJson(objectMeta));
                } catch (Exception e) {
                    throw new ConnectFilePulseException(
                        "Failed to compute object-file key while initializing processing for '" + uri + "'. " +
                        " Connector must be restated.",
                        e
                    );
                }

                iterables.add(new DelegateFileInputIterator(key, uri, reader));
                if (hasListener()){
                    listener.onScheduled(new FileObjectContext(key, objectMeta));
                }
            // Else, object-file does NOT exist or is not readable.
            } else {
                try {
                    // try to compute offset using GenericFileObjectMeta for notifying connector.
                    final GenericFileObjectMeta objectMeta = new GenericFileObjectMeta(uri);
                    final FileObjectKey key = FileObjectKey.of(offsetPolicy.toPartitionJson(objectMeta));
                    if (hasListener()){
                        LOG.warn("Object-file does not exist or is not readable. Skip and continue '{}'", uri);
                        listener.onInvalid(new FileObjectContext(key, objectMeta));
                    }
                } catch (Exception e) {
                    throw new ConnectFilePulseException(
                        "Failed to compute object-file key while initializing processing for '" + uri + "'. " +
                        "Connector must be restated.",
                        e
                    );
                }
            }
        }
        queue.addAll(iterables);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectContext context() {
        if (currentIterator != null) {
            FileObjectContext context = currentIterator.context();
            if (latestPolledRecord != null) {
                context = new FileObjectContext(
                        context.key(),
                        context.metadata(),
                        latestPolledRecord.offset().toSourceOffset());
            }
            return context;
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        if (isClosed()) {
            throw new IllegalStateException("FileRecordsPollingConsumer is closed, no more element can be returned");
        }

        if (!hasNext())
            return RecordsIterable.empty();

        currentIterator = findNextFileObjectIterator();

        if (currentIterator == null)
            return RecordsIterable.empty();

        Exception exception = null;
        try {
            // Read the next records from the current iterator
            final RecordsIterable<FileRecord<TypedStruct>> records = currentIterator.next();

            // Apply the filter-chain in the returned records
            final RecordsIterable<FileRecord<TypedStruct>> filtered = pipeline.apply(
                    records,
                    currentIterator.hasNext()
            );
            // May update the last polled records.
            if (!filtered.isEmpty()) {
                latestPolledRecord = filtered.last();
            }

            // Return record to SourceTask
            return filtered;
        } catch (final ConnectFilePulseException e) {
            exception = e;
            throw e;

        } catch (final Exception e) {
            exception = e;
            throw new ConnectFilePulseException(e);

        } finally {
            if (exception != null) {
                LOG.error(
                    "Stopped processing due to error during filter-chain execution for object-file: '{}'",
                    currentIterator.context().metadata()
                );
                closeIterator(currentIterator, exception);
            }
        }
    }

    private FileInputIterator<FileRecord<TypedStruct>> findNextFileObjectIterator() {

        if (queue.isEmpty()) return null;

        // Quickly iterate to lookup for a valid iterator
        FileInputIterator<FileRecord<TypedStruct>> ret = null;
        do {
            final DelegateFileInputIterator candidate = queue.peek();
            var objectMeta = new GenericFileObjectMeta(candidate.getObjectURI());

            if (candidate.isOpen()) {
                ret = getOrCloseIteratorIfNoMoreRecord(candidate);
            } else {
                try {
                    // Re-check if the object-file still exists before opening a new iterator.
                    if (!candidate.isValid()) {
                        LOG.warn(
                            "Object-file does not exist or is not readable. Skip and continue '{}'",
                            candidate.getObjectURI());
                        queue.remove();
                        listener.onInvalid(new FileObjectContext(candidate.key(), objectMeta));
                        continue;
                    }
                    ret = openAndGetIteratorOrNullIfCompleted(candidate);
                    if (ret == null) {
                        // Remove the current iterator and continue
                        deleteFileQueueAndInvokeListener(new FileObjectContext(candidate.key(), objectMeta), null);
                    }
                } catch (Exception e) {
                    LOG.error(
                        "Failed to open and initialize new iterator for object-file: {}.",
                        candidate.getObjectURI()
                    );
                    deleteFileQueueAndInvokeListener(new FileObjectContext(candidate.key(), objectMeta), e);
                    // Rethrow the exception.
                    throw e;
                }
            }
        } while (hasNext() && ret == null);

        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            DelegateFileInputIterator monitor;
            while ((monitor = queue.poll()) != null) {
                try {
                    monitor.close();
                } catch (Exception ignore) {

                }
            }
            reader.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStateListener(final StateListener listener) {
        this.listener = listener;
    }

    /**
     * Attempt to initialize a new records iterator for the specified iterator.
     * The iterator will automatically seek to the latest committed offset.
     * <p>
     * This method will return {@code null} if the iterator point to an object-file
     * which has already been completed file.
     *</p>
     * @param iterator the source file iterator
     * @return a new {@link FileInputIterator} instance or {@code null} if the iterator is already completed.
     */
    private FileInputIterator<FileRecord<TypedStruct>> openAndGetIteratorOrNullIfCompleted(
            final DelegateFileInputIterator iterator
    ) {
        FileObjectMeta metadata = null;
        FileObjectOffset committedOffset;
        try {
            metadata = iterator.getMetadata();

            if (!ignoreCommittedOffsets) {
                committedOffset = offsetPolicy
                        .getOffsetFor(taskContext, metadata)
                        .orElse(FileObjectOffset.empty());
            } else {
                committedOffset = FileObjectOffset.empty();
            }
        } catch (final Exception e) {
            if (metadata != null) {
                LOG.warn(
                        "Failed to load committed offset for object file {}. " +
                        "Previous offset will be ignored. Error: {}",
                        iterator.getObjectURI(),
                        e.getMessage()
                );
                committedOffset = FileObjectOffset.empty();
            } else {
                // Rethrow the exception if metadata cannot be loaded.
                // The exception handling logic will be delegated to the calling method.
                throw e;
            }
        }

        // Quickly check if we can consider this file completed based on the content-length.
        boolean isAlreadyCompleted = committedOffset.position() >= metadata.contentLength();
        if (!ignoreCommittedOffsets && isAlreadyCompleted) {
            LOG.warn(
                "Detected object-file already completed. Skip entry and continue '{}'",
                iterator.getObjectURI()
            );
            // Return NULL so that the calling method can properly close the iterator.
            return null;
        }

        iterator.open();
        iterator.seekTo(committedOffset);
        pipeline.init(iterator.context());
        if (hasListener()) {
            listener.onStart(iterator.context());
        }
        return iterator;
    }

    private FileInputIterator<FileRecord<TypedStruct>> getOrCloseIteratorIfNoMoreRecord(
            final DelegateFileInputIterator iterable) {

        // then check if there is still records to consume.
        if (!iterable.hasNext()) {
            // close otherwise.
            closeIterator(iterable, null);
        } else {
            return iterable;
        }
        return null;
    }

    public void closeCurrentIterator(final Exception cause) {
        closeIterator(currentIterator, cause);
    }

    private void closeIterator(final FileInputIterator<FileRecord<TypedStruct>> iterator,
                               final Exception cause) {
        final FileObjectContext context = iterator.context();
        try {
            iterator.close();
        } catch (final Exception e) {
            LOG.debug("Error while closing iterator for: '{}'", context.metadata(), e);
        } finally {
            deleteFileQueueAndInvokeListener(context, cause);
        }
    }

    private void deleteFileQueueAndInvokeListener(final FileObjectContext fileContext,
                                                  final Throwable exception) {
        queue.remove();
        if (hasListener()) {
            if (exception != null) {
                listener.onFailure(fileContext, exception);
            } else {
                listener.onCompleted(fileContext);
            }
        }
    }

    private boolean hasListener() {
        return listener != null;
    }
}