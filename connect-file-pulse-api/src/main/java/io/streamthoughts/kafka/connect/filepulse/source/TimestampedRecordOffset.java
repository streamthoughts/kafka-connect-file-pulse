/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Objects;

public abstract class TimestampedRecordOffset implements FileRecordOffset {

    private final long timestamp;

    /**
     * Creates a new {@link TimestampedRecordOffset} instance.
     *
     * @param timestamp     the current timestamp.
     */
    protected TimestampedRecordOffset(final long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Returns the creation time for this.
     *
     * @return a unix-timestamp in millisecond.
     */
    public long timestamp() {
        return this.timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimestampedRecordOffset)) return false;
        TimestampedRecordOffset that = (TimestampedRecordOffset) o;
        return timestamp == that.timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract FileObjectOffset toSourceOffset();
}
