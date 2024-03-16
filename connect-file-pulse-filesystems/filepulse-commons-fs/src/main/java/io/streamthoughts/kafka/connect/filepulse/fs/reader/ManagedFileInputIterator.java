/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ManagedFileInputIterator<T> implements FileInputIterator<FileRecord<T>> {

    private final AtomicBoolean closed;

    /**
     * The iterator-manager.
     */
    private final IteratorManager iteratorManager;

    protected FileObjectContext context;

    /**
     * Creates a new {@link ManagedFileInputIterator} instance.
     *
     * @param objectMeta        The file's metadata.
     * @param iteratorManager   The iterator manager.
     */
    public ManagedFileInputIterator(final FileObjectMeta objectMeta,
                                    final IteratorManager iteratorManager) {
        this.iteratorManager =  Objects.requireNonNull(iteratorManager, "iteratorManager can't be null");
        this.closed = new AtomicBoolean(false);
        this.context = new FileObjectContext(objectMeta);
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
    public void close() {
        if (!isClosed()) {
            iteratorManager.removeIterator(this);
            closed.set(true);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return closed.get();
    }
}
