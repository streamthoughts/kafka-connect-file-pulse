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

import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link IteratorManager} can be used to easily close all open {@link FileInputIterator} instances.
 */
public class IteratorManager {

    private final Set<FileInputIterator<?>> openIterators;

    /**
     * Creates a new {@link IteratorManager} instance.
     */
    public IteratorManager() {
        this.openIterators = Collections.synchronizedSet(new HashSet<>());
    }

    /**
     * Add the specified iterator to this manager.
     *
     * @param iterator  an iterator to manage.
     */
    void addOpenIterator(final FileInputIterator<?> iterator) {
        this.openIterators.add(iterator);
    }

    /**
     * Remove the specified iterator from this manager.
     *
     * @param iterator  an iterator to remove.
     */
    void removeIterator(final FileInputIterator<?> iterator) {
        if (iterator.isClosed()) {
            openIterators.remove(iterator);
        }
    }

    /**
     * Close all registered {@link FileInputIterator} instance.
     */
    void closeAll() {
        this.openIterators.forEach(FileInputIterator::close);
    }
}
