/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractFileInputIterator<T> implements FileInputIterator<FileRecord<T>> {

    private final AtomicBoolean closed;

    /**
     * The iterator-manager.
     */
    private final IteratorManager iteratorManager;

    /**
     * The input-source context.
     */
    protected FileContext context;

    /**
     * Creates a new {@link AbstractFileInputIterator} instance.
     *
     * @param iteratorManager   the {@link IteratorManager} instance.
     * @param context           the {@link FileContext} instance.
     */
    public AbstractFileInputIterator(final IteratorManager iteratorManager,
                                     final FileContext context) {
        Objects.requireNonNull(iteratorManager, "iteratorManager can't be null");
        Objects.requireNonNull(context, "context can't be null");
        this.iteratorManager = iteratorManager;
        this.context = context;
        closed = new AtomicBoolean(false);
    }

    @Override
    public FileContext context() {
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!isClose()) {
            iteratorManager.removeIterator(this);
            closed.set(true);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClose() {
        return closed.get();
    }
}
