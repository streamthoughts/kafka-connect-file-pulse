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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.ManagedFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal.TextBlock;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowFileInputIterator extends ManagedFileInputIterator<TypedStruct> {

    private static final Logger LOG = LoggerFactory.getLogger(RowFileInputIterator.class);

    /**
     * The buffer reader.
     */
    private final NonBlockingBufferReader reader;

    /**
     * The minimum number of lines to read before returning records.
     */
    private int minNumReadRecords = 1;

    private long offsetLines = 0L;

    private long maxWaitMs = 0L;

    private long lastObservedRecords;

    /**
     /**
     * Creates a new {@link RowFileInputIterator} instance.
     *
     * @param meta              the {@link FileObjectMeta}.
     * @param reader            the {@link NonBlockingBufferReader}.
     * @param iteratorManager   the {@link IteratorManager}.
     */
    public RowFileInputIterator(final FileObjectMeta meta,
                                final IteratorManager iteratorManager,
                                final NonBlockingBufferReader reader) {
        super(meta, iteratorManager);
        this.reader = Objects.requireNonNull(reader, "reader can't be null");
        lastObservedRecords = Time.SYSTEM.milliseconds();
    }

    public RowFileInputIterator setMinNumReadRecords(final int minNumReadRecords) {
        this.minNumReadRecords = minNumReadRecords;
        return this;
    }

    public RowFileInputIterator setMaxWaitMs(final long maxWaitMs) {
        this.maxWaitMs = maxWaitMs;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
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
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        try {
            mayWaitForLinesToBeAvailable();
            List<FileRecord<TypedStruct>> records = new LinkedList<>();
            List<TextBlock> lines = reader.readLines(minNumReadRecords, false);
            if (lines != null) {
                for (TextBlock line : lines) {
                    offsetLines++;
                    TypedStruct struct = TypedStruct.create();
                    struct.put(TypedFileRecord.DEFAULT_MESSAGE_FIELD, line.data());
                    final FileRecordOffset offset = RowFileRecordOffset
                            .with(line.startOffset(), line.endOffset())
                            .withSize(line.size())
                            .withRowNumber(offsetLines);
                    records.add(new TypedFileRecord(offset, struct));
                }
            }
            if (!records.isEmpty() && canWaitForMoreRecords()) {
                // Only update lastObservedRecords if no more record is expected to be read,
                // otherwise the next iteration will be performed.
                lastObservedRecords = Time.SYSTEM.milliseconds();
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

    private void mayWaitForLinesToBeAvailable() {
        if (!reader.hasNext()) {
            LOG.debug("Waiting for more bytes from file {} (timeout={}ms)", context.metadata(), maxWaitMs);
            final long timeout = lastObservedRecords + maxWaitMs;
            while (!reader.hasNext() && canWaitForMoreRecords()) {
                Time.SYSTEM.sleep(Math.min(100, Math.abs(timeout - Time.SYSTEM.milliseconds())));
            }
        }

        if (!reader.hasNext() && !canWaitForMoreRecords()) {
            LOG.info(
                "Timeout after waiting for more bytes from file {} after '{}ms'.",
                context.metadata(),
                maxWaitMs
            );
            if (reader.remaining()) {
                LOG.info("Remaining buffered bytes detected");
                reader.enableAutoFlush();
            }
        }
    }

    private void updateContext() {
        final FileObjectOffset offset = new FileObjectOffset(
            reader.position(),
            offsetLines,
            Time.SYSTEM.milliseconds());
        context = context.withOffset(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return reader.hasNext() ||
               reader.remaining() ||
               canWaitForMoreRecords();
    }

    private boolean canWaitForMoreRecords() {
        return lastObservedRecords + maxWaitMs > Time.SYSTEM.milliseconds();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!isClosed()) {
            if (this.reader != null) {
                this.reader.close();
            }
            super.close();
        }
    }
}
