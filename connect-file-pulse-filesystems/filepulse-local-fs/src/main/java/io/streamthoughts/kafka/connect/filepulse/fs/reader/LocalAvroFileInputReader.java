/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.avro.AvroDataFileIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * The {@link LocalAvroFileInputReader} can be used for reading data records from an Avro container file.
 */
public class LocalAvroFileInputReader extends BaseLocalFileInputReader {

    /**
     * Creates a new {@link LocalAvroFileInputReader} instance.
     */
    public LocalAvroFileInputReader() {
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
            final File file = new File(objectURI);
            return new AvroDataFileIterator(metadata, iteratorManager, file);
        } catch (ReaderException | IOException e) {
            throw new ReaderException("Failed to create a new AvroDataFileIterator for:" + objectURI, e);
        }
    }
}