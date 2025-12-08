/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Map;

/**
 * Strategy interface for determining when a file should be marked as COMPLETED.
 */
public interface FileCompletionStrategy {

    /**
     * Configure this strategy.
     *
     * @param configs the configuration properties.
     */
    default void configure(final Map<String, ?> configs) {
        // Default: no-op
    }

    /**
     * Check if the file should be marked as completed based on the strategy.
     *
     * @param context the file object context.
     * @return true if the file should be marked as COMPLETED, false otherwise.
     */
    boolean shouldComplete(final FileObjectContext context);
}