/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link IteratorManager} can be used to easily close all open {@link FileInputIterator} instances.
 */
public class IteratorManager {

    private final Set<FileInputIterator<?>> openIterators;

    /**
     * Creates a new {@link IteratorManager} instance.
     */
    public IteratorManager() {
        this.openIterators = Collections.synchronizedSet(new HashSet<>());
    }

    /**
     * Add the specified iterator to this manager.
     *
     * @param iterator  an iterator to manage.
     */
    void addOpenIterator(final FileInputIterator<?> iterator) {
        this.openIterators.add(iterator);
    }

    /**
     * Remove the specified iterator from this manager.
     *
     * @param iterator  an iterator to remove.
     */
    void removeIterator(final FileInputIterator<?> iterator) {
        if (iterator.isClosed()) {
            openIterators.remove(iterator);
        }
    }

    /**
     * Close all registered {@link FileInputIterator} instance.
     */
    void closeAll() {
        this.openIterators.forEach(FileInputIterator::close);
    }
}
