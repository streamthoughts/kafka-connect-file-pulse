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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.avro;

import static io.streamthoughts.kafka.connect.filepulse.internal.Silent.unchecked;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.ManagedFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.internal.Silent;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Time;

public class AvroDataFileIterator extends ManagedFileInputIterator<TypedStruct> {

    private long recordsReadSinceLastSync = 0L;

    private long lastSync = -1L;

    private final DataFileReader<GenericRecord> reader;

    /**
     * Creates a new {@link AvroDataFileIterator} instance.
     *
     * @param iteratorManager the {@link IteratorManager} instance.
     * @param objectMeta      the {@link FileObjectMeta} instance.
     */
    public AvroDataFileIterator(final FileObjectMeta objectMeta,
                                final IteratorManager iteratorManager,
                                final File avroFile) throws IOException {
        super(objectMeta, iteratorManager);
        this.reader = new DataFileReader<>(avroFile, new GenericDatumReader<>());;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        Objects.requireNonNull(offset, "offset can't be null");
        if (offset.position() != -1) {
            unchecked(() -> reader.seek(offset.position()), ReaderException::new);
            recordsReadSinceLastSync = 0L;
            lastSync = reader.previousSync();
            skipRecordsUntil(offset.rows());
        }
    }

    /**
     * Skips records until {@link #recordsReadSinceLastSync} equal the specified records.
     *
     * @param records the number of records to skip.
     */
    private void skipRecordsUntil(long records) {
        while (recordsReadSinceLastSync < records) {
            nextRecord();
        }
    }

    /**
     * Updates the current {@link #context}.
     */
    private void updateContext() {
        final FileObjectOffset offset = new FileObjectOffset(
                lastSync,
                recordsReadSinceLastSync,
                Time.SYSTEM.milliseconds());
        context = context.withOffset(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        try {
            final GenericRecord record = nextRecord();
            final TypedStruct struct = AvroTypedStructConverter.fromGenericRecord(record);

            AvroRecordOffset offset = new AvroRecordOffset(
                    lastSync,
                    position(),
                    recordsReadSinceLastSync
            );

            return RecordsIterable.of(new TypedFileRecord(offset, struct));
        } finally {
            updateContext();
        }
    }

    /**
     * Read the next records.
     *
     * @return a {@link GenericRecord} instance.
     */
    private GenericRecord nextRecord() {
        // start to read a new block.
        if (reader.previousSync() != lastSync) {
            lastSync = reader.previousSync();
            recordsReadSinceLastSync = 0;
        }

        final GenericRecord record = reader.next();
        recordsReadSinceLastSync++;
        return record;
    }

    private long position() {
        return Silent.unchecked(reader::tell, ReaderException::new);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!isClosed()) {
            Silent.unchecked(reader::close, ReaderException::new);
            super.close();
        }
    }
}