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
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileInputIterable implements Iterable<RecordsIterable<FileRecord<TypedStruct>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FileInputIterable.class);

    private final URI source;
    private final FileInputReader reader;
    private FileInputIterator<FileRecord<TypedStruct>> iterator;
    private final FileObjectMeta metadata;

    private final AtomicBoolean isOpen = new AtomicBoolean(false);

    /**
     * Creates a new {@link FileInputIterable} instance.
     *
     * @param source the input source file.
     * @param reader the input source reader used to create a new {@link FileInputIterator}.
     */
    FileInputIterable(final URI source, final FileInputReader reader) {
        this.source = Objects.requireNonNull(source, "source can't be null");;
        this.reader = Objects.requireNonNull(reader, "reader can't be null");;
        this.metadata = reader.readMetadata(source);
    }

    /**
     * Opens a new iterator.
     *
     * @param offset    the offset to seek the iterator.
     * @return a new {@link FileInputIterator} instance.
     */
    public FileInputIterator<FileRecord<TypedStruct>> open(final FileObjectOffset offset) {
        LOG.info("Opening new iterator for source : {}", metadata);
        iterator = reader.newIterator(new FileContext(metadata));
        iterator.seekTo(offset);
        isOpen.set(true);
        return iterator;
    }

    public FileObjectMeta metadata() {
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> iterator() {
        checkState();
        return iterator;
    }

    boolean isOpen() {
        return isOpen.get() && !iterator.isClose();
    }

    boolean isValid() {
        return reader.isReadable(source);
    }

    private void checkState() {
        if (!isOpen()) {
            throw new IllegalStateException("iterator is not open");
        }
    }

    void close() {
        if (isOpen.get()) {
            LOG.info("Closing iterator for source {} ", metadata().uri());
            iterator.close();
        }
    }

    static boolean isAlreadyCompleted(final FileObjectOffset committedOffset, final FileObjectMeta metadata) {
        return committedOffset != null && committedOffset.position() >= metadata.contentLength();
    }

}