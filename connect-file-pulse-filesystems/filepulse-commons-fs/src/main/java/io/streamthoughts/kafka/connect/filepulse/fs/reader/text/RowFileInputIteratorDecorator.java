/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.util.Objects;

/**
 * An abstract class that can be used to decorate a {@link FileInputIterator}.
 */
public abstract class RowFileInputIteratorDecorator implements FileInputIterator<FileRecord<TypedStruct>> {

    protected final FileInputIterator<FileRecord<TypedStruct>> iterator;

    /**
     * Creates a new {@link RowFileInputIteratorDecorator} instance.
     *
     * @param iterator  the {@link FileInputIterator} to decorate.
     */
    public RowFileInputIteratorDecorator(final FileInputIterator<FileRecord<TypedStruct>> iterator) {
        this.iterator = Objects.requireNonNull(iterator, "iterator should not be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectContext context() {
        return iterator.context();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        iterator.seekTo(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        iterator.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return iterator.isClosed();
    }
}
