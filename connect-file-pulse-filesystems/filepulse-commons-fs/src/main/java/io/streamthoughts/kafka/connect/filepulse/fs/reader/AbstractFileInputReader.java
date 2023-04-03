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
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractFileInputReader implements FileInputReader {

    private final AtomicBoolean isClosed;

    private final IteratorManager openIterators;

    /**
     * Creates a new {@link AbstractFileInputReader} instance.
     */
    public AbstractFileInputReader() {
        this.isClosed = new AtomicBoolean(false);
        this.openIterators = new IteratorManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI) {
        final FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectURI, openIterators);
        openIterators.addOpenIterator(iterator);
        return iterator;
    }

    protected abstract FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                              final IteratorManager iteratorManager);

    protected IteratorManager iteratorManager() {
        return openIterators;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!isClosed.get()) {
            openIterators.closeAll();
        }
    }
}
