/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TimestampedRecordOffset;
import java.util.Objects;

public class BytesRecordOffset extends TimestampedRecordOffset {

    private final long startPosition;

    private final long endPosition;

    public static BytesRecordOffset empty() {
        return new BytesRecordOffset(
                -1,
                -1,
                System.currentTimeMillis()
        );
    }

    /**
     * Creates a new {@link BytesRecordOffset} instance.
     *
     * @param startPosition the starting position.
     * @param endPosition   the ending position.
     */
    public BytesRecordOffset(long startPosition,
                             long endPosition) {
        this(startPosition, endPosition, System.currentTimeMillis());
    }

    /**
     * Creates a new {@link BytesRecordOffset} instance.
     *
     * @param startPosition the starting position.
     * @param endPosition   the ending position.
     * @param timestamp     the current timestamp.
     */
    public BytesRecordOffset(long startPosition,
                             long endPosition,
                             long timestamp) {
        super(timestamp);
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }
    /**
     * Returns the starting position.
     *
     * @return the long position.
     */
    public long startPosition() {
        return this.startPosition;
    }


    /**
     * Returns the ending position.
     *
     * @return the long position.
     */
    public long endPosition() {
        return this.endPosition;
    }

    public FileObjectOffset toSourceOffset() {
        return new FileObjectOffset(endPosition, -1, timestamp());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BytesRecordOffset)) return false;
        if (!super.equals(o)) return false;
        BytesRecordOffset that = (BytesRecordOffset) o;
        return startPosition == that.startPosition &&
                endPosition == that.endPosition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), startPosition, endPosition);
    }

    @Override
    public String toString() {
        return "[" +
                "startPosition=" + startPosition +
                ", endPosition=" + endPosition +
                ", timestamp=" + timestamp() +
                ']';
    }
}
