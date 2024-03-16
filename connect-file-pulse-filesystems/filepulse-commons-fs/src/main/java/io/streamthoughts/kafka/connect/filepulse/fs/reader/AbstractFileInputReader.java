/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractFileInputReader implements FileInputReader {

    private final AtomicBoolean isClosed;

    private final IteratorManager openIterators;

    /**
     * Creates a new {@link AbstractFileInputReader} instance.
     */
    public AbstractFileInputReader() {
        this.isClosed = new AtomicBoolean(false);
        this.openIterators = new IteratorManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI) {
        final FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectURI, openIterators);
        openIterators.addOpenIterator(iterator);
        return iterator;
    }

    protected abstract FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                              final IteratorManager iteratorManager);

    protected IteratorManager iteratorManager() {
        return openIterators;
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
}
