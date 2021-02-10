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
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.BytesRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.reader.AbstractFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

/**
 *
 * The {@code BytesArrayInputReader} creates one record per input file.
 * Each record has single field {@code message} containing the content of the file as a byte array.
 */
public class LocalBytesArrayInputReader extends BaseLocalFileInputReader {

    /**
     * Creates a new {@link LocalBytesArrayInputReader} instance.
     */
    public LocalBytesArrayInputReader() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileContext context,
                                                                     final IteratorManager iteratorManager) {
        return new BytesArrayInputIterator(
                context,
                iteratorManager
        );
    }

    public static class BytesArrayInputIterator extends AbstractFileInputIterator<TypedStruct> {


        private boolean hasNext = true;

        /**
         * Creates a new {@link BytesArrayInputIterator} instance.
         *
         * @param context           the {@link FileContext} to be used for this iterator.
         * @param iteratorManager   the {@link IteratorManager} instance used for managing this iterator.
         */
        BytesArrayInputIterator(final FileContext context,
                                final IteratorManager iteratorManager) {
            super(iteratorManager, context);
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

            final URI uri = context().metadata().uri();
            try {
                byte[] bytes = Files.readAllBytes(Paths.get(uri));
                TypedStruct struct = TypedStruct.create().put(TypedFileRecord.DEFAULT_MESSAGE_FIELD, bytes);
                return RecordsIterable.of(new TypedFileRecord(new BytesRecordOffset(0, bytes.length), struct));
            } catch (IOException e) {
                throw new ReaderException("Failed to read file:  " + uri, e);
            } finally {
                hasNext = false;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return !isClose() && hasNext;
        }
    }
}
