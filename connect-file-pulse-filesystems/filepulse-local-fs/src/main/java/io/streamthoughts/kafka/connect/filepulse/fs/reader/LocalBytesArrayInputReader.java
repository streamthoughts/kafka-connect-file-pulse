/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.BytesArrayInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;

/**
 *
 * The {@code BytesArrayInputReader} creates one record per input file.
 * Each record has single field {@code message} containing the content of the file as a byte array.
 */
public class LocalBytesArrayInputReader extends BaseLocalFileInputReader {

    private BytesArrayInputIteratorFactory factory;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        factory = new BytesArrayInputIteratorFactory(storage(), iteratorManager());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                     final IteratorManager iteratorManager) {
        return factory.newIterator(objectURI);
    }
}
