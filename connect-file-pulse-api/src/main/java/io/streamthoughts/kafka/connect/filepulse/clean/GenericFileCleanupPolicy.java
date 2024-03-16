/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.fs.StorageAware;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * Top level interface for cleanup policies.
 *
 * @param <S>   type of the source to be cleaned.
 * @param <R>   type of the operation result.
 */
public interface GenericFileCleanupPolicy<S, R> extends AutoCloseable, Configurable, StorageAware {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    void configure(final Map<String, ?> configs);

    /**
     * Applies the cleanup policy on the specified input files in success.
     *
     * @param source    the {@link S} to be cleaned.
     * @return the operation result.
     */
    R apply(final S source);

    /**
     * Close any internal resources.
     */
    @Override
    default void close() throws Exception {

    }
}
