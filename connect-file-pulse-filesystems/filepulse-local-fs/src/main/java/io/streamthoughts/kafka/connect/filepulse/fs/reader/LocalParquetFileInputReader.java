/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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