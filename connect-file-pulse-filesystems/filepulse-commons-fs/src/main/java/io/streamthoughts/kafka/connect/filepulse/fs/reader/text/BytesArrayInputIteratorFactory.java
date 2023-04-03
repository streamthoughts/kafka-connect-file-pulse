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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Objects;

public class BytesArrayInputIteratorFactory implements FileInputIteratorFactory {

    private final Storage storage;
    private final IteratorManager iteratorManager;

    /**
     * Creates a new {@link BytesArrayInputIteratorFactory} instance.
     *
     * @param storage   the {@link Storage}.
     */
    public BytesArrayInputIteratorFactory(final Storage storage,
                                          final IteratorManager iteratorManager) {
        this.storage = Objects.requireNonNull(storage, "storage should not be null");
        this.iteratorManager = Objects.requireNonNull(iteratorManager, "iteratorManager should not be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI) {

        try {
            return new BytesArrayInputIterator(
                storage.getObjectMetadata(objectURI),
                storage.getInputStream(objectURI),
                iteratorManager
            );
        } catch (Exception e) {
            throw new ReaderException("Failed to create BytesArrayInputIterator for: " + objectURI, e);
        }
    }
}
