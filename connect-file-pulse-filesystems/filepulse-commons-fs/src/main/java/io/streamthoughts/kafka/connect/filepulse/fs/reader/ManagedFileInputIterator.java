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

import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ManagedFileInputIterator<T> implements FileInputIterator<FileRecord<T>> {

    private final AtomicBoolean closed;

    /**
     * The iterator-manager.
     */
    private final IteratorManager iteratorManager;

    protected FileObjectContext context;

    /**
     * Creates a new {@link ManagedFileInputIterator} instance.
     *
     * @param objectMeta        The file's metadata.
     * @param iteratorManager   The iterator manager.
     */
    public ManagedFileInputIterator(final FileObjectMeta objectMeta,
                                    final IteratorManager iteratorManager) {
        this.iteratorManager =  Objects.requireNonNull(iteratorManager, "iteratorManager can't be null");
        this.closed = new AtomicBoolean(false);
        this.context = new FileObjectContext(objectMeta);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectContext context() {
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!isClosed()) {
            iteratorManager.removeIterator(this);
            closed.set(true);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return closed.get();
    }
}
