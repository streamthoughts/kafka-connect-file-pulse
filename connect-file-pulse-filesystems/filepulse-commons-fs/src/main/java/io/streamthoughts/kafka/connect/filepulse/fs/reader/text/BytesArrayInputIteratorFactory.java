/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Objects;

public class BytesArrayInputIteratorFactory implements FileInputIteratorFactory {

    private final Storage storage;
    private final IteratorManager iteratorManager;

    /**
     * Creates a new {@link BytesArrayInputIteratorFactory} instance.
     *
     * @param storage   the {@link Storage}.
     */
    public BytesArrayInputIteratorFactory(final Storage storage,
                                          final IteratorManager iteratorManager) {
        this.storage = Objects.requireNonNull(storage, "storage should not be null");
        this.iteratorManager = Objects.requireNonNull(iteratorManager, "iteratorManager should not be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI) {

        try {
            return new BytesArrayInputIterator(
                storage.getObjectMetadata(objectURI),
                storage.getInputStream(objectURI),
                iteratorManager
            );
        } catch (Exception e) {
            throw new ReaderException("Failed to create BytesArrayInputIterator for: " + objectURI, e);
        }
    }
}
