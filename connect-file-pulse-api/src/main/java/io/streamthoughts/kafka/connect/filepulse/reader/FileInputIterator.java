/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import java.util.Iterator;

/**
 * Default interface to iterate over an input file.
 *
 * @param <T> type of value.
 */
public interface FileInputIterator<T> extends Iterator<RecordsIterable<T>>, AutoCloseable {

    /**
     * Gets the iterator context.
     * @return  a {@link FileObjectContext} instance.
     */
    FileObjectContext context();

    /**
     * Seeks iterator to the specified startPosition.
     * @param offset  the position from which to seek the iterator.
     */
    void seekTo(final FileObjectOffset offset);

    /**
     * Reads the next records from the iterator file.
     */
    RecordsIterable<T> next();

    /**
     * Checks whether the iterator file has more records to read.
     * @return {@code true} if the iteration has more elements
     */
    boolean hasNext();

    /**
     * Close the iterator file/input stream.
     */
    void close();

    /**
     * Checks whether this iterator is already close.
     * @return {@code true} if this iterator is close.
     */
    boolean isClosed();
}
