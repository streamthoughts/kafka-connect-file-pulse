/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.avro.AvroDataStreamIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;

/**
 * The {@code AliyunOSSAvroFileInputReader} can be used to created records from an AVRO file loaded from Aliyun OSS.
 */
public class AliyunOSSAvroFileInputReader extends BaseAliyunOSSInputReader {

    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                     final IteratorManager iteratorManager) {

        try {
            final FileObjectMeta metadata = storage().getObjectMetadata(objectURI);
            return new AvroDataStreamIterator(metadata, iteratorManager, storage().getInputStream(objectURI));

        } catch (Exception e) {
            throw new ReaderException("Failed to create AvroDataStreamIterator for: " + objectURI, e);
        }
    }
}
