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
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DelegateFileInputIterator}.
 */
public class DelegateFileInputIterator implements FileInputIterator<FileRecord<TypedStruct>> {

    private static final Logger LOG = LoggerFactory.getLogger(DelegateFileInputIterator.class);

    private final URI objectURI;
    private final FileObjectKey key;
    private final FileInputReader reader;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private FileInputIterator<FileRecord<TypedStruct>> iterator;

    /**
     * Creates a new {@link DelegateFileInputIterator} instance.
     *
     * @param key       the object file key.
     * @param objectURI the object file URI.
     * @param reader    the input source reader used to create a new {@link FileInputIterator}.
     */
    DelegateFileInputIterator(final FileObjectKey key,
                              final URI objectURI,
                              final FileInputReader reader) {
        this.key = Objects.requireNonNull(key, "'key' should not be null");
        this.objectURI = Objects.requireNonNull(objectURI, "'objectURI' can't be null");
        this.reader = Objects.requireNonNull(reader, "'reader' can't be null");
    }

    /**
     * Gets the metadata of the backed object file.
     *
     * @return the {@link FileObjectMeta}
     */
    public FileObjectMeta getMetadata() {
        return reader.getObjectMetadata(objectURI);
    }

    /**
     * Gets the URI of the backed object file.
     *
     * @return the {@link URI}
     */
    public URI getObjectURI() {
        return objectURI;
    }

    /**
     * Gets the {@link FileInputIterator} or create a new one if none has been initialized yet.
     */
    public void open() {
        if (isOpen()) throw new IllegalStateException("Iterator is already open");
        LOG.info("Opening new iterator for: {}", objectURI);
        iterator = reader.newIterator(objectURI);
    }

    /**
     * @return {@code true} if an iterator is already opened.
     */
    boolean isOpen() {
        return iterator != null && !iterator.isClosed();
    }

    /**
     * @return {@code true} if the backed object file can be read and is accessible.
     */
    boolean isValid() {
        return reader.canBeRead(objectURI);
    }

    public FileObjectKey key() {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectContext context() {
        if (iterator == null) {
            throw new IllegalStateException("Iterator is not initialized for URI: " + objectURI);
        }
        final FileObjectContext context = iterator.context();
        return new FileObjectContext(
                key,
                context.metadata(),
                context.offset()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        checkIsOpen();
        iterator.seekTo(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        checkIsOpen();
        return iterator.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        checkIsOpen();
        return iterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            iterator.close();
            LOG.info("Closed iterator for: {} ", iterator.context().metadata());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    private void checkIsOpen() {
        if (!isOpen()) throw new IllegalStateException("This iterator is not initialized yet or already closed");
    }

}