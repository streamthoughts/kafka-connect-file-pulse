/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.avro;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IndexRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.ManagedFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.internal.Silent;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroDataStreamIterator extends ManagedFileInputIterator<TypedStruct> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDataStreamIterator.class);

    private final DataFileStream<GenericRecord> stream;

    private long position = 0;

    /**
     * Creates a new {@link AvroDataStreamIterator} instance.
     *
     * @param objectMeta        The file's metadata.
     * @param iteratorManager   The iterator manager.
     * @param stream            the file's input streams.
     */
    public AvroDataStreamIterator(final FileObjectMeta objectMeta,
                                  final IteratorManager iteratorManager,
                                  final InputStream stream) throws IOException {
        super(objectMeta, iteratorManager);
        this.stream = new DataFileStream<>(stream, new GenericDatumReader<>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        Objects.requireNonNull(offset, "offset can't be null");
        if (offset.position() != -1) {
            LOG.info("Seeking to skip to Avro record {}", offset.position() );
            position = (int) offset.position();
            int i = 0;
            while (i < position) {
                stream.next();
                i++;
            }
        }
    }

    /**
     * Updates the current {@link #context}.
     */
    private void updateContext() {
        context = context.withOffset(new FileObjectOffset(position, position, Time.SYSTEM.milliseconds()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        try {
            final GenericRecord record = stream.next();
            return incrementAndGet(AvroTypedStructConverter.fromGenericRecord(record));
        } finally {
            updateContext();
        }
    }

    private RecordsIterable<FileRecord<TypedStruct>> incrementAndGet(final TypedStruct struct) {
        position++;
        return RecordsIterable.of(new TypedFileRecord(new IndexRecordOffset(position), struct));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return stream.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!isClosed()) {
            Silent.unchecked(stream::close);
            super.close();
        }
    }
}