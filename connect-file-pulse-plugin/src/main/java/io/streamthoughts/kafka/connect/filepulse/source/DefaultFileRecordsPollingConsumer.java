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
import io.streamthoughts.kafka.connect.filepulse.filter.FilterException;
import io.streamthoughts.kafka.connect.filepulse.filter.RecordFilterPipeline;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    private FileRecord<TypedStruct> latestPollRecord;

    private FileInputIterator<FileRecord<TypedStruct>> currentIterator;

    /**
     * Creates a new {@link DefaultFileRecordsPollingConsumer} instance.
     *
     * @param taskContext            the current task context.
     * @param reader                 the reader to be used.
     * @param pipeline               the filter pipeline to apply on each records.
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
        if (isClose()) {
            throw new IllegalStateException("Can't add new input files, consumer is closed");
        }
        final List<DelegateFileInputIterator> iterables = files
                .stream()
                .map(uri -> new DelegateFileInputIterator(uri, reader))
                .filter(excludeUnreadableAndNotify())
                .peek(it -> {
                    if (hasListener()) listener.onScheduled(new FileContext(it.getMetadata()));
                })
                .collect(Collectors.toList());
        queue.addAll(iterables);
    }

    private Predicate<DelegateFileInputIterator> excludeUnreadableAndNotify() {
        return it -> {
            boolean valid = it.isValid();
            if (!valid) {
                LOG.error("Invalid source, file doesn't exist or is not readable - ignore : {}", it.getObjectURI());
                listener.onInvalid(new FileContext(new GenericFileObjectMeta(it.getObjectURI())));
            }
            return valid;
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileContext context() {
        if (currentIterator != null) {
            FileContext context = currentIterator.context();
            if (latestPollRecord != null) {
                context = new FileContext(
                        context.metadata(),
                        latestPollRecord.offset().toSourceOffset());
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
        if (queue.isEmpty()) return RecordsIterable.empty();

        // Quickly iterate to lookup for a valid iterator
        do {
            final DelegateFileInputIterator candidate = queue.peek();
            if (candidate.isOpen()) {
                currentIterator = getOrCloseIteratorIfNoMoreRecord(candidate);
            } else {
                currentIterator = openAndGetIteratorOrNullIfInvalid(taskContext, candidate);
            }
        } while (!queue.isEmpty() && currentIterator == null);

        if (currentIterator == null) {
            return RecordsIterable.empty();
        }

        final RecordsIterable<FileRecord<TypedStruct>> records = currentIterator.next();

        Exception exception = null;
        try {
            final RecordsIterable<FileRecord<TypedStruct>> filtered = pipeline.apply(
                    records,
                    currentIterator.hasNext()
            );
            if (!filtered.isEmpty()) {
                latestPollRecord = filtered.last();
            }
            return filtered;
        } catch (final FilterException e) {
            exception = e;
            // ignore the error - and skip the current file.
            return RecordsIterable.empty();

        } catch (final ConnectFilePulseException e) {
            exception = e;
            throw e;

        } catch (final Exception e) {
            exception = e;
            throw new ConnectFilePulseException(e);

        } finally {
            if (exception != null) {
                closeIterator(currentIterator, exception);
            }
        }
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
        DelegateFileInputIterator monitor;
        while ((monitor = queue.poll()) != null) {
            try {
                monitor.close();
            } catch (Exception ignore) {

            }
        }
        reader.close();
        closed.set(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClose() {
        return closed.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFileListener(final StateListener listener) {
        this.listener = listener;
    }

    /**
     * Attempt to initialize a new records iterator for the specified iterator.
     * The iterator will automatically seek to the latest committed offset.
     * <p>
     * This method will return {@code null} if the iterator point
     * to either an invalid file or to an already been completed file.
     *
     * @param context  the connect source task context
     * @param iterator the source file iterator
     * @return a new {@link FileInputIterator} instance or {@code null} if the iterator is invalid.
     */
    private FileInputIterator<FileRecord<TypedStruct>> openAndGetIteratorOrNullIfInvalid(
            final SourceTaskContext context,
            final DelegateFileInputIterator iterator
    ) {
        // Re-check if the file is still valid.

        if (!iterator.isValid()) {
            LOG.error("File does not exist or is not readable, skip entry and continue '{}'", iterator.getObjectURI());

            var metadata = new GenericFileObjectMeta(iterator.getObjectURI());
            deleteFileQueueAndInvokeListener(new FileContext(metadata), null);
            return null;
        }

        try {
            final FileObjectMeta metadata = iterator.getMetadata();

            final FileObjectOffset committedOffset;
            if (!ignoreCommittedOffsets) {
                committedOffset = offsetPolicy.getOffsetFor(context, metadata).orElse(FileObjectOffset.empty());
            } else {
                committedOffset = FileObjectOffset.empty();
            }

            // Quickly check if we can considered this file completed based on the content-length.
            boolean isAlreadyCompleted = committedOffset.position() >= metadata.contentLength();
            if (!ignoreCommittedOffsets && isAlreadyCompleted) {
                LOG.warn(
                        "Detected source file already completed, skip entry and continue '{}'",
                        iterator.getObjectURI());
                deleteFileQueueAndInvokeListener(new FileContext(metadata, committedOffset), null);
            } else {
                iterator.open();
                iterator.seekTo(committedOffset);
                pipeline.init(iterator.context());
                if (hasListener()) {
                    listener.onStart(iterator.context());
                }
                return iterator;
            }
        } catch (final Exception e) {
            FileObjectMeta metadata;
            try {
                // Attempt to load metadata for listener
                metadata = iterator.getMetadata();
            } catch (Exception ignore) {
                metadata = new GenericFileObjectMeta(iterator.getObjectURI());;
            }
            deleteFileQueueAndInvokeListener(new FileContext(metadata), e);
        }

        return null;
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

    private void closeIterator(final FileInputIterator<FileRecord<TypedStruct>> iterator,
                               final Exception cause) {
        try {
            LOG.info("Closing iterator for: {} ", iterator.context().metadata());
            iterator.close();
        } catch (final Exception e) {
            LOG.debug("Error while closing iterator for: '{}'", iterator.context().metadata(), e);
        } finally {
            deleteFileQueueAndInvokeListener(iterator.context(), cause);
        }
    }

    private void deleteFileQueueAndInvokeListener(final FileContext taskContext,
                                                  final Throwable exception) {
        queue.remove();
        if (hasListener()) {
            if (exception != null) {
                listener.onFailure(taskContext, exception);
            } else {
                listener.onCompleted(taskContext);
            }
        }
    }

    private boolean hasListener() {
        return listener != null;
    }
}