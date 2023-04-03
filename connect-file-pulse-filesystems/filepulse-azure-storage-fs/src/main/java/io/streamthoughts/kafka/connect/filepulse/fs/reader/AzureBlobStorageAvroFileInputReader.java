/*
 * Copyright 2019-2021 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.fs.reader.avro.AvroDataStreamIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;

public class AzureBlobStorageAvroFileInputReader extends AzureBlobStorageInputReader {

    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(URI objectURI, IteratorManager iteratorManager) {
        try {
            final FileObjectMeta metadata = storage.getObjectMetadata(objectURI);
            return new AvroDataStreamIterator(
                    metadata,
                    iteratorManager,
                    storage().getInputStream(objectURI)
            );

        } catch (Exception e) {
            throw new ReaderException("Failed to create AvroDataStreamIterator for: " + objectURI, e);
        }
    }
}
