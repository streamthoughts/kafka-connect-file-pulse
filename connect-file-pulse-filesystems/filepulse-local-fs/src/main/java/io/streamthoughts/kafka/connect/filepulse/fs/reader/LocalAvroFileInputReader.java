/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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