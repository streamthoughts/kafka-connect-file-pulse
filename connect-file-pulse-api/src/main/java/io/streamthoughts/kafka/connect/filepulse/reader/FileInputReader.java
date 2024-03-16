/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * A {@code FileInputReader} is the principal class used to read an input file/object.
 */
public interface FileInputReader extends FileInputIteratorFactory, Configurable, AutoCloseable {

    /**
     * Configure this class with the given key-value pairs.
     *
     * @param configs the reader configuration.
     */
    @Override
    default void configure(final Map<String, ?> configs) {

    }

    /**
     * Gets the metadata for the source object identified by the given {@link URI}.
     *
     * @param objectURI   the {@link URI} of the file object.
     * @return            a new {@link FileObjectMeta} instance.
     */
    FileObjectMeta getObjectMetadata(final URI objectURI);

    /**
     * Checks whether the source object identified by the given {@link URI} can be read.
     *
     * @param objectURI   the {@link URI} of the file object.
     * @return            the {@code true}.
     */
    boolean canBeRead(final URI objectURI);

    /**
     * Creates a new {@link FileInputIterator} for the given {@link URI}.
     *
     * @param objectURI   the {@link URI} of the file object.
     * @return          a new {@link FileInputIterator} iterator instance.
     *
     */
    FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI);

    /**
     * Close this reader and any remaining un-close iterators.
     */
    @Override
    void close();
}
