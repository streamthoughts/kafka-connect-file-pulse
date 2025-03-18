/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import com.jsoniter.annotation.JsonCreator;
import com.jsoniter.annotation.JsonProperty;
import java.util.Objects;

/**
 * An object representing the position of next bytes to read in the input source.
 */
public class FileObjectOffset {

    private final long position;

    private final long rows;

    private final long timestamp;

    public static FileObjectOffset empty() {
        return new FileObjectOffset(-1, 0, System.currentTimeMillis());
    }

    /**
     * Creates a new {@link FileObjectOffset} instance.
     *
     * @param position   the position of next bytes to read in the input source.
     * @param rows       the number of rows already read from the input source.
     * @param timestamp  the current timestamp.
     */
    @JsonCreator
    public FileObjectOffset(@JsonProperty("position") long position,
                            @JsonProperty("rows") long rows,
                            @JsonProperty("timestamp") long timestamp) {
        this.position = position;
        this.rows = rows;
        this.timestamp = timestamp;
    }

    /**
     * Returns the positions of next bytes to read in the input source.
     *
     * @return the long position.
     */
    public long position() {
        return this.position;
    }

    /**
     * Returns the number of rows already read from the input source.
     *
     * @return the number of rows.
     */
    public long rows() {
        return this.rows;
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
        if (!(o instanceof FileObjectOffset)) return false;
        FileObjectOffset that = (FileObjectOffset) o;
        return position == that.position &&
                rows == that.rows &&
                timestamp == that.timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(position, rows, timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "position=" + position +
                ", rows=" + rows +
                ", timestamp=" + timestamp +
                ']';
    }
}