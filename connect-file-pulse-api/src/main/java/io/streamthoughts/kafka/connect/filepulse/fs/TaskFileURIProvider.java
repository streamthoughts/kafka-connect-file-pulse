/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
