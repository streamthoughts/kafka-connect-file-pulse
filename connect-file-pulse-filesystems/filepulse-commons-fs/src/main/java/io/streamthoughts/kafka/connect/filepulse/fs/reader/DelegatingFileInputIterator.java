/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.filter.FilterContext;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DelegatingFileInputIterator implements FileInputIterator<FileRecord<TypedStruct>> {

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Iterator<TypedFileRecord> iterator;
    private final FileObjectContext context;

    /**
     * Creates a new {@link DelegatingFileInputIterator} instance.
     *
     * @param context   the {@link FilterContext} object.
     * @param iterator  the {@link Iterator} to delegate.
     */
    DelegatingFileInputIterator(final FileObjectContext context,
                                final Iterator<TypedFileRecord> iterator) {
        this.context = context;
        this.iterator = iterator;
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
    public void seekTo(FileObjectOffset offset) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        return RecordsIterable.of(iterator.next());
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
        isClosed.set(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return isClosed.get();
    }
}
