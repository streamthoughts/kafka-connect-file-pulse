/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

/**
 * A {@link FileCompletionStrategy} that marks files as COMPLETED
 * immediately when they are fully read.
 * This is the default behavior and maintains backward compatibility.
 */
public class EofCompletionStrategy implements FileCompletionStrategy {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldComplete(final FileObjectContext context) {
        // Complete immediately when the file is fully read
        return true;
    }
}