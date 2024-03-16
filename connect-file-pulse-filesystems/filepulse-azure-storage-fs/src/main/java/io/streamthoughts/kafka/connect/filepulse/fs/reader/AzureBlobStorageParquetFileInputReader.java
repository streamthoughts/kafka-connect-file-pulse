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
import java.net.URI;

public class AzureBlobStorageParquetFileInputReader extends AzureBlobStorageInputReader {

    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(URI objectURI, IteratorManager iteratorManager) {
        try {
            final FileObjectMeta metadata = storage.getObjectMetadata(objectURI);
            return new ParquetFileInputIterator(
                    metadata,
                    iteratorManager,
                    storage().getInputStream(objectURI)
            );

        } catch (Exception e) {
            throw new ReaderException("Failed to create ParquetFileInputIterator for: " + objectURI, e);
        }
    }
}
