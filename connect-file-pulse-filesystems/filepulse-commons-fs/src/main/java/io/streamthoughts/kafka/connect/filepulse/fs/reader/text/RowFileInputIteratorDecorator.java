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
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;

import java.util.Objects;

/**
 * An abstract class that can be used to decorate a {@link FileInputIterator}.
 */
public abstract class RowFileInputIteratorDecorator implements FileInputIterator<FileRecord<TypedStruct>> {

    protected final FileInputIterator<FileRecord<TypedStruct>> iterator;

    /**
     * Creates a new {@link RowFileInputIteratorDecorator} instance.
     *
     * @param iterator  the {@link FileInputIterator} to decorate.
     */
    public RowFileInputIteratorDecorator(final FileInputIterator<FileRecord<TypedStruct>> iterator) {
        this.iterator = Objects.requireNonNull(iterator, "iterator should not be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectContext context() {
        return iterator.context();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        iterator.seekTo(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        iterator.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return iterator.isClosed();
    }
}
