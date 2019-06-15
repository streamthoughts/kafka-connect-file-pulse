/*
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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.offset.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import io.streamthoughts.kafka.connect.filepulse.reader.internal.NonBlockingBufferReader;
import io.streamthoughts.kafka.connect.filepulse.reader.internal.TextBlock;
import io.streamthoughts.kafka.connect.filepulse.reader.internal.ReversedInputFileReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RowFileInputIterator implements FileInputIterator<FileInputRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(RowFileInputIterator.class);

    private static final String HEADERS_RECORD_FIELD = "headers";
    private static final String FOOTERS_RECORD_FIELD = "footers";

    /**
     * The input-source context.
     */
    private FileInputContext context;

    /**
     * The buffer reader.
     */
    private final NonBlockingBufferReader reader;

    /**
     * The minimum number of lines to read before returning records.
     */
    private int minNumReadRecords = 0;

    /**
     * The number of rows to be skipped in the beginning of file.
     */
    private int skipHeaders = 0;

    /**
     * The number of rows to be skipped at the end of file.
     */
    private int skipFooters = 0;

    private final IteratorManager iteratorManager;

    private List<TextBlock> headers;
    private List<String> headerStrings;

    private List<TextBlock> footers;
    private List<String> footersStrings;

    private long offsetLines = 0L;

    private final Charset charset;

    private long maxWaitMs = 0L;

    private AtomicBoolean closed = new AtomicBoolean(false);

    private Schema schema;

    private AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     /**
     * Creates a new {@link RowFileInputIterator} instance.
     *
     * @param context           the text file context.
     * @param reader            the buffer reader.
     * @param iteratorManager   the iterator manager.
     */
    private RowFileInputIterator(final FileInputContext context,
                                 final NonBlockingBufferReader reader,
                                 final IteratorManager iteratorManager,
                                 final Charset charset) {
        Objects.requireNonNull(context, "context can't be null");
        Objects.requireNonNull(reader, "reader can't be null");
        Objects.requireNonNull(iteratorManager, "iteratorManager can't be null");
        Objects.requireNonNull(charset, "charset can't be null");
        this.context = context;
        this.reader = reader;
        this.iteratorManager = iteratorManager;
        this.charset = charset;
    }

    private void setMinNumReadRecords(final int minNumReadRecords) {
        this.minNumReadRecords = minNumReadRecords;
    }

    private void setSkipHeaders(final int skipHeaders) {
        this.skipHeaders = skipHeaders;
    }

    private void setSkipFooters(final int skipFooters) {
        this.skipFooters = skipFooters;
    }

    private void setMaxWaitMs(final long maxWaitMs) {
        this.maxWaitMs = maxWaitMs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final SourceOffset offset) {
        Objects.requireNonNull(offset, "offset can't be null");
        if (offset.position() != -1) {
            offsetLines = offset.rows();
            reader.seekTo(offset.position());
        }
        updateContext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputContext context() {
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileInputRecord> next() {
        try {
            initializeIfNeeded();
            List<FileInputRecord> records = new LinkedList<>();
            List<TextBlock> lines = reader.readLines(minNumReadRecords);
            if (lines != null) {
                for (TextBlock line : lines) {
                    offsetLines++;
                    if (isNotLineFooter(line) && isNotLineHeader(line)) {
                        records.add(createOutputRecord(line));
                    }
                }
            }
            return new RecordsIterable<>(records);
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        } finally {
            updateContext();
        }
        return null;
    }

    private void updateContext() {
        final SourceOffset offset = new SourceOffset(
                reader.position(),
                offsetLines,
                Time.SYSTEM.milliseconds());
        context = context.withOffset(offset);
    }

    private FileInputRecord createOutputRecord(final TextBlock record) {

        Struct struct = new Struct(schema);
        struct.put(FileInputData.DEFAULT_MESSAGE_FIELD, record.data());
        if (skipHeaders > 0) {
            struct.put(HEADERS_RECORD_FIELD, headerStrings);
        }

        if (skipFooters > 0) {
            struct.put(FOOTERS_RECORD_FIELD, footersStrings);
        }

        final FileInputOffset offset = new FileInputOffset(
                record.startOffset(),
                record.endOffset(),
                offsetLines,
                Time.SYSTEM.milliseconds(),
                record.size());
        return new FileInputRecord(offset, new FileInputData(struct));
    }

    private void initializeIfNeeded() {
        if (!initialized.get()) {
            mayReadHeaders();
            mayReadFooters();

            final SchemaBuilder builder = FileInputData.defaultSchema();
            final SchemaBuilder array = SchemaBuilder.array(SchemaBuilder.string());
            if (skipHeaders > 0) {
                builder.field(HEADERS_RECORD_FIELD, array).optional();
            }

            if (skipFooters > 0) {
                builder.field(FOOTERS_RECORD_FIELD, array).optional();
            }
            schema = builder.build();
            initialized.set(true);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        boolean hasNext = reader.hasNext();
        if (hasNext) return true;

        LOG.debug("Waiting for more bytes from file {} (timeout={}ms)", context.metadata(), maxWaitMs);
        long timeout = Time.SYSTEM.milliseconds() + maxWaitMs;
        do {
            Time.SYSTEM.sleep(Math.min(100, Math.abs(timeout - Time.SYSTEM.milliseconds())));
            hasNext = reader.hasNext();
        } while (!hasNext && Time.SYSTEM.milliseconds() < timeout );

        if (!hasNext) {
            LOG.info(
                "Timeout after waiting for more bytes from file {} after '{}ms'.",
                context.metadata(),
                maxWaitMs);
            if (reader.remaining()) {
                LOG.info("Remaining buffered bytes detected");
                reader.enableAutoFlush();
                hasNext = true;
            }
        }
        return hasNext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!closed.get()) {
            if (this.reader != null) {
                this.reader.close();
            }
            iteratorManager.removeIterator(this);
            closed.set(true);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClose() {
        return closed.get();
    }

    private void mayReadFooters() {
        final String fileName = context.metadata().name();
        final String path = context.metadata().absolutePath();
        if (skipFooters > 0) {
            LOG.info("Starting to read footer lines ({}) from file {}", skipFooters, fileName);

            try (final ReversedInputFileReader reversedReader = new ReversedInputFileReader(path, charset)) {
                footers = reversedReader.readLines(skipFooters);
            } catch (Exception e) {
                throw new RuntimeException("", e);
            }
            if (footers.size() < skipFooters) {
                throw new ReaderException("Not enough data for reading footer lines from file "
                        + path
                        + " (available=" + footers.size() + ", expecting=" + skipFooters + ")");
            }
            Collections.reverse(footers);
            footersStrings = footers.stream().map(TextBlock::data).collect(Collectors.toList());
        }
    }

    private void mayReadHeaders() {
        final String fileName = context.metadata().name();
        final String path = context.metadata().absolutePath();
        if (skipHeaders > 0) {
            LOG.info("Starting to read header lines ({}) from file {}", skipHeaders, fileName);
            try (final NonBlockingBufferReader sequentialReader =
                    new NonBlockingBufferReader(new File(path), charset)) {
                headers = sequentialReader.readLines(skipHeaders);
                headerStrings = headers
                    .stream()
                    .map(TextBlock::data)
                    .collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("", e);
            }
            if (headers.size() < skipHeaders) {
                throw new ReaderException(
                    String.format(
                        "Not enough data for reading header lines from file %s (available=%d, expecting=%d)",
                        path,
                        headers.size(),
                        skipHeaders)
                );
            }
        }
    }

    private boolean isNotLineHeader(final TextBlock source) {
        return skipHeaders <= 0 ||
               source.startOffset() > headers.get(skipHeaders - 1).startOffset();
    }

    private boolean isNotLineFooter(final TextBlock source) {
        return skipFooters <= 0 ||
               source.startOffset() < footers.get(0).startOffset();
    }

    /**
     * Simple class to build a new {@link RowFileInputIterator}.
     */
    public static class Builder {

        private Charset charset;
        private int minNumReadRecords;
        private FileInputContext context;
        private int initialBufferSize;
        private int skipHeaders;
        private int skipFooters;
        private long waitMaxMs;
        private IteratorManager iteratorManager;

        /**
         * Creates a new {@link Builder} instance.
         */
        private Builder() {
            this.charset = StandardCharsets.UTF_8;
            this.minNumReadRecords = 1;
            this.initialBufferSize = NonBlockingBufferReader.DEFAULT_INITIAL_CAPACITY;
        }

        Builder withContext(final FileInputContext context) {
            this.context = context;
            return this;
        }

        Builder withSkipHeaders(final int skipHeaders) {
            this.skipHeaders = skipHeaders;
            return this;
        }

        Builder withSkipFooters(final int skipFooters) {
            this.skipFooters = skipFooters;
            return this;
        }

        Builder withMinNumReadRecords(final int minNumReadRecords) {
            this.minNumReadRecords = minNumReadRecords;
            return this;
        }

        Builder withMaxWaitMs(final long maxWaitMs) {
            this.waitMaxMs = maxWaitMs;
            return this;
        }

        Builder withInitialBufferSize(final int initialBufferSize) {
            this.initialBufferSize = initialBufferSize;
            return this;
        }

        Builder withCharset(final Charset charset) {
            this.charset = charset;
            return this;
        }

        Builder withIteratorManager(final IteratorManager iteratorManager) {
            this.iteratorManager = iteratorManager;
            return this;
        }

        RowFileInputIterator build() {
            validateNotNull(context, "context");
            NonBlockingBufferReader reader = new NonBlockingBufferReader(context.file(), initialBufferSize, charset);
            reader.disableAutoFlush();
            RowFileInputIterator iterator = new RowFileInputIterator(context, reader, iteratorManager, charset);
            iterator.setSkipFooters(skipFooters);
            iterator.setSkipHeaders(skipHeaders);
            iterator.setMinNumReadRecords(minNumReadRecords);
            iterator.setMaxWaitMs(waitMaxMs);
            return iterator;
        }

        private void validateNotNull(final Object o, final String property) {
            if (o == null) {
                throw new IllegalStateException(
                    "Error while building new RowFileInputIterator. The property " + property +" is null.");
            }
        }

    }

    /**
     * Returns a new {@link Builder} instance.
     *
     * @return a new {@link Builder} instance.
     */
    static Builder newBuilder() {
        return new Builder();
    }
}
