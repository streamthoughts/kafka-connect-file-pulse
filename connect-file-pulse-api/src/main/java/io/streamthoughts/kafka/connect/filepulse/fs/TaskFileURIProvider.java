/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * Class that can be used to provide to {@link org.apache.kafka.connect.source.SourceTask}
 * the next URIs of the object files to process.
 */
public interface TaskFileURIProvider extends Configurable {

    /**
     * {@inheritDoc}
     */
    @Override
    default void configure(final Map<String, ?> configs) { }

    /**
     * Retrieves the {@link URI}s of the next object files to read.
     *
     * @throws java.util.NoSuchElementException if the provider has no more elements.
     * @return the URIs of the object file to read.
     */
    List<URI> nextURIs();

    /**
     * Returns {@code true} if the provider has more URIs.
     * (In other words, returns {@code true} if {@link #nextURIs} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the provider has more URIs.
     */
    boolean hasMore();
    
    /**
     * Close underlying I/O resources.
     */
    default void close() {

    }
}
