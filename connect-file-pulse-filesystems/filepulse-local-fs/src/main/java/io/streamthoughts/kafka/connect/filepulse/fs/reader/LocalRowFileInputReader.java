/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;

/**
 * The {@code RowFileInputReader} reads an input local file line by line.
 */
public class LocalRowFileInputReader extends BaseLocalFileInputReader {

    private RowFileInputIteratorFactory factory;

    /**
     * Creates a new {@link LocalRowFileInputReader} instance.
     */
    public LocalRowFileInputReader() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.factory = new RowFileInputIteratorFactory(
            new RowFileInputIteratorConfig(configs),
            storage(),
            iteratorManager()
        );
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                  final IteratorManager iteratorManager) {
        return factory.newIterator(objectURI);
    }
}
