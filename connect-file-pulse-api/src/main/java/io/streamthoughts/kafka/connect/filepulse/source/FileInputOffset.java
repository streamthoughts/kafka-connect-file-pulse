/*
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
package io.streamthoughts.kafka.connect.filepulse.source;

import org.apache.kafka.common.utils.SystemTime;

import java.util.Objects;

/**
 *
 */
public class FileInputOffset {

    private final long startPosition;

    private final long endPosition;

    private final long rows;

    private final long timestamp;

    private final long size;

    public static FileInputOffset empty() {
        return new FileInputOffset(-1, -1,0, SystemTime.SYSTEM.milliseconds(), -1);
    }

    public static FileInputOffset with(long startPosition, long endPosition) {
        return new FileInputOffset(startPosition, endPosition,0, SystemTime.SYSTEM.milliseconds(), -1);
    }

    /**
     * Creates a new {@link FileInputOffset} instance.
     *
     * @param startPosition the starting position.
     * @param endPosition   the ending position.
     * @param rows          the number of rows already read from the input source.
     * @param timestamp     the current timestamp.
     */
    public FileInputOffset(long startPosition,
                           long endPosition,
                           long rows,
                           long timestamp,
                           long size) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.rows = rows;
        this.timestamp = timestamp;
        this.size = size;
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

    public SourceOffset toSourceOffset() {
        return new SourceOffset(endPosition, rows, timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileInputOffset)) return false;
        FileInputOffset that = (FileInputOffset) o;
        return startPosition == that.startPosition &&
                endPosition == that.endPosition &&
                rows == that.rows &&
                timestamp == that.timestamp &&
                size == that.size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(startPosition, endPosition, rows, timestamp, size);
    }

    @Override
    public String toString() {
        return "[" +
                "startPosition=" + startPosition +
                ", endPosition=" + endPosition +
                ", rows=" + rows +
                ", timestamp=" + timestamp +
                ", size=" + size +
                ']';
    }
}