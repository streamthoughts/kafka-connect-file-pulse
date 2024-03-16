/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Arrays;

/**
 * An enum representing the status for a {@link FileObject}.
 */
public enum FileObjectStatus {
    /**
     * The file has been scheduled by the connector thread.
     */
    SCHEDULED,

    /**
     * The file can't be scheduled because it is not readable.
     */
    INVALID,

    /**
     * The file is starting to be processed by a task.
     */
    STARTED,

    /**
     * The is file is currently being read by a task.
     */
    READING,

    /**
     * The file processing is completed.
     */
    COMPLETED,

    /**
     * The completed file is committed.
     */
    COMMITTED,

    /**
     * The file processing failed.
     */
    FAILED,

    /**
     * The file has been successfully clean up (depending on the configured strategy).
     */
    CLEANED;

    public boolean isOneOf(final FileObjectStatus...states) {
        return Arrays.asList(states).contains(this);
    }

    public boolean isDone() {
        return isOneOf(COMMITTED, FAILED, CLEANED);
    }
}
