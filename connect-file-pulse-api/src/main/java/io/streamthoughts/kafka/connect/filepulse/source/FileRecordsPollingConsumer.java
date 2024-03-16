/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;


import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;

/**
 *
 */
public interface FileRecordsPollingConsumer<T> extends FileInputIterator<T> {

    /**
     * Returns the context for the last record return from the {@link #next()} method.
     * Can return {@code null} if the {@link #next()} method has never been invoked.
     *
     * @return  a {@link FileObjectContext} instance.
     */
    FileObjectContext context();

    /**
     * Sets a state listener.
     *
     * @param listener  the {@link StateListener} instance to be used.
     */
    void setStateListener(final StateListener listener);
}
