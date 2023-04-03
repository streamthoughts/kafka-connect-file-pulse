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
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

public class BytesArrayInputIterator extends ManagedFileInputIterator<TypedStruct> {

    private final InputStream stream;

    private boolean hasNext = true;

    private final FileObjectContext context;

    /**
     * Creates a new {@link BytesArrayInputIterator} instance.
     *
     * @param meta              the {@link FileObjectMeta meta}.
     * @param stream            the {@link InputStream}.
     * @param iteratorManager   the {@link IteratorManager} instance used for managing this iterator.
     */
    BytesArrayInputIterator(final FileObjectMeta meta,
                            final InputStream stream,
                            final IteratorManager iteratorManager) {
        super(meta, iteratorManager);
        this.stream = stream;
        this.context = new FileObjectContext(meta);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectContext context() {
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try (stream){
            byte[] bytes =  stream.readAllBytes();
            TypedStruct struct = TypedStruct.create().put(TypedFileRecord.DEFAULT_MESSAGE_FIELD, bytes);
            return RecordsIterable.of(new TypedFileRecord(new BytesRecordOffset(0, bytes.length), struct));
        } catch (IOException e) {
            throw new ReaderException("Failed to read all bytes from:  " + context.metadata(), e);
        } finally {
            hasNext = false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return !isClosed() && hasNext;
    }
}
