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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * A {@code FileInputReader} is the principal class used to read an input file/object.
 */
public interface FileInputReader extends FileInputIteratorFactory, Configurable, AutoCloseable {

    /**
     * Configure this class with the given key-value pairs.
     *
     * @param configs the reader configuration.
     */
    @Override
    default void configure(final Map<String, ?> configs) {

    }

    /**
     * Gets the metadata for the source object identified by the given {@link URI}.
     *
     * @param objectURI   the {@link URI} of the file object.
     * @return            a new {@link FileObjectMeta} instance.
     */
    FileObjectMeta getObjectMetadata(final URI objectURI);

    /**
     * Checks whether the source object identified by the given {@link URI} can be read.
     *
     * @param objectURI   the {@link URI} of the file object.
     * @return            the {@code true}.
     */
    boolean canBeRead(final URI objectURI);

    /**
     * Creates a new {@link FileInputIterator} for the given {@link URI}.
     *
     * @param objectURI   the {@link URI} of the file object.
     * @return          a new {@link FileInputIterator} iterator instance.
     *
     */
    FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI);

    /**
     * Close this reader and any remaining un-close iterators.
     */
    @Override
    void close();
}
