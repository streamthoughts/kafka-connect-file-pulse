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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads all bytes from an input files.
 */
public class BytesArrayInputReader implements FileInputReader {

    private final AtomicBoolean isClosed;

    private final IteratorManager openIterators;

    /**
     * Creates a new {@link BytesArrayInputReader} instance.
     */
    public BytesArrayInputReader() {
        this.isClosed = new AtomicBoolean(false);
        this.openIterators = new IteratorManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        /**/
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileContext context) {
        return new BytesArrayInputIterator(
                context,
                openIterators
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!isClosed.get()) {
            openIterators.closeAll();
        }
    }

    public static class BytesArrayInputIterator implements FileInputIterator<FileRecord<TypedStruct>> {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final IteratorManager iteratorManager;
        private final FileContext context;

        private boolean hasNext = true;

        /**
         * Creates a new {@link BytesArrayInputIterator} instance.
         *
         * @param context           the {@link FileContext} to be used for this iterator.
         * @param iteratorManager   the {@link IteratorManager} instance used for managing this iterator.
         */
        BytesArrayInputIterator(final FileContext context,
                                final IteratorManager iteratorManager) {
            this.iteratorManager = iteratorManager;
            this.context = context;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileContext context() {
            return context;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void seekTo(final SourceOffset offset) {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RecordsIterable<FileRecord<TypedStruct>> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                final File file = context.file();
                final Path path = file.toPath();
                byte[] bytes = Files.readAllBytes(path);

                TypedStruct struct = new TypedStruct();
                struct.put(TypedFileRecord.DEFAULT_MESSAGE_FIELD, bytes);

                final FileRecordOffset offset = FileRecordOffset.with(0, bytes.length);
                
                return RecordsIterable.of(new TypedFileRecord(offset, struct));
            } catch (IOException e) {
                throw new ReaderException("Error while reading file :  " + context.metadata(), e);
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

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            if (!closed.get()) {
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
    }
}
