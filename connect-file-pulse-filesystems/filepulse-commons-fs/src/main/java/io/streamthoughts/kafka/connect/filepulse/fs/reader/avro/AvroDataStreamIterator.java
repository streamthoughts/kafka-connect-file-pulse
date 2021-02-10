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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IndexRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.internal.Silent;
import io.streamthoughts.kafka.connect.filepulse.reader.AbstractFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class AvroDataStreamIterator extends AbstractFileInputIterator<TypedStruct> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDataStreamIterator.class);

    private final DataFileStream<GenericRecord> stream;

    private long position = 0;

    /**
     * Creates a new {@link AvroDataStreamIterator} instance.
     *
     * @param iteratorManager the {@link IteratorManager} instance.
     * @param context         the {@link FileContext} instance.
     */
    public AvroDataStreamIterator(final IteratorManager iteratorManager,
                                  final FileContext context,
                                  final InputStream stream) throws IOException {
        super(iteratorManager, context);
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
        if (!isClose()) {
            Silent.unchecked(stream::close);
            super.close();
        }
    }
}