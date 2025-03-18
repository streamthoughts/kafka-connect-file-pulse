/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import java.util.Objects;

/**
 * Represents the position of a record into a text XML file.
 */
public class RowFileRecordOffset extends BytesRecordOffset {

    private final long rows;

    private final long size;

    public static RowFileRecordOffset empty() {
        return new RowFileRecordOffset(
                -1,
                -1,
                0,
                System.currentTimeMillis(),
                0
        );
    }

    public static RowFileRecordOffset with(long startPosition, long endPosition) {
        return new RowFileRecordOffset(
                startPosition,
                endPosition,
                0,
                System.currentTimeMillis(),
                endPosition - startPosition
        );
    }

    /**
     * Creates a new {@link RowFileRecordOffset} instance.
     *
     * @param startPosition the starting position.
     * @param endPosition   the ending position.
     * @param rows          the number of rows already read from the input source.
     * @param timestamp     the current timestamp.
     */
    public RowFileRecordOffset(long startPosition,
                               long endPosition,
                               long rows,
                               long timestamp,
                               long size) {
        super(timestamp, startPosition, endPosition);
        this.rows = rows;
        this.size = size;
    }

    public RowFileRecordOffset withSize(long size) {
        return new RowFileRecordOffset(
            startPosition(),
            endPosition(),
            rows,
            timestamp(),
            size
        );
    }

    public RowFileRecordOffset withRowNumber(long number) {
        return new RowFileRecordOffset(
            startPosition(),
            endPosition(),
            number,
            timestamp(),
            size
        );
    }

    /**
     * Returns the number of rows already read from the input source.
     *
     * @return the number of rows.
     */
    public long rows() {
        return this.rows;
    }

    public FileObjectOffset toSourceOffset() {
        return new FileObjectOffset(endPosition(), rows, timestamp());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowFileRecordOffset)) return false;
        if (!super.equals(o)) return false;
        RowFileRecordOffset that = (RowFileRecordOffset) o;
        return rows == that.rows &&
                size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rows, size);
    }

    @Override
    public String toString() {
        return "[" +
                "startPosition=" + startPosition() +
                ", endPosition=" + endPosition() +
                ", rows=" + rows +
                ", timestamp=" + timestamp() +
                ", size=" + size +
                ']';
    }
}
