/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.parquet.ParquetFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * The {@link LocalParquetFileInputReader} can be used for reading data records from a parquet container file.
 */
public class LocalParquetFileInputReader extends BaseLocalFileInputReader {

    /**
     * Creates a new {@link LocalParquetFileInputReader} instance.
     */
    public LocalParquetFileInputReader() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                     final IteratorManager iteratorManager) {
        try {
            final FileObjectMeta metadata = storage().getObjectMetadata(objectURI);
            final InputStream inputStream = storage().getInputStream(objectURI);
            return new ParquetFileInputIterator(metadata, iteratorManager, inputStream);
        } catch (ReaderException | IOException e) {
            throw new ReaderException("Failed to create a new ParquetFileInputIterator for:" + objectURI, e);
        }
    }
}