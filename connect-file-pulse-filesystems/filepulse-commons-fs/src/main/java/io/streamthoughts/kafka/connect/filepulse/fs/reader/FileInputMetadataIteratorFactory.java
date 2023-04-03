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
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.BytesRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;

public class FileInputMetadataIteratorFactory implements FileInputIteratorFactory {

    public static final String METADATA_RECORD_NAME = "io.streamthoughts.kafka.connect.filepulse.FileMetadata";

    private final Storage storage;

    /**
     * Creates a new {@link FileInputMetadataIteratorFactory} instance.
     *
     * @param storage the {@link Storage} to use for getting metadata.
     */
    public FileInputMetadataIteratorFactory(final Storage storage) {
        this.storage = Objects.requireNonNull(storage, "storage should not be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI) {
            final FileObjectMeta metadata = storage.getObjectMetadata(objectURI);
            final TypedStruct struct = TypedStruct.create(METADATA_RECORD_NAME)
                    .put("name", metadata.name())
                    .put("uri", metadata.stringURI())
                    .put("contentDigest", metadata.contentDigest().digest())
                    .put("contentDigestAlgorithm", metadata.contentDigest().algorithm())
                    .put("lastModified", metadata.lastModified())
                    .put("size", metadata.contentLength())
                    .put("metadata", metadata.userDefinedMetadata());

            TypedFileRecord record = new TypedFileRecord(BytesRecordOffset.empty(), struct);
            return new DelegatingFileInputIterator(
                    new FileObjectContext(metadata),
                    Collections.singleton(record).iterator()
            );
    }
}
