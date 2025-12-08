/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional strategy interface that can be implemented alongside {@link FileCompletionStrategy}
 * to influence when the connector should attempt to read from continuously appended files.
 *
 * <p>This is intended for long-lived, append-only files (for example, daily log files) where
 * it may be desirable to back off read attempts when no new data is expected for some time,
 * in order to avoid unnecessary polling or timeouts while still allowing the completion
 * strategy to control when the file is eventually marked as COMPLETED.
 */
public interface LongLivedFileReadStrategy {

    Logger LOG = LoggerFactory.getLogger(LongLivedFileReadStrategy.class);

    /**
     * Determines whether the connector should attempt to read from the given file
     * based on its current context.
     *
     * <p>The default implementation checks if the file has been modified since
     * the last read offset timestamp.
     *
     * @param objectMeta the file object metadata.
     * @param offset     the last read offset for the file.
     * @return true if the connector should attempt to read from the file, false otherwise.
     */
    default boolean shouldAttemptRead(final FileObjectMeta objectMeta, final FileObjectOffset offset) {
        return objectMeta.lastModified() > offset.timestamp();
    }
}