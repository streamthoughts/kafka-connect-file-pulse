/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.parquet;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TimestampedRecordOffset;
import java.util.Objects;
import org.apache.kafka.common.utils.Time;

/**
 * Represents the position of a record into an Parquet container file.
 */
public class ParquetRecordOffset extends TimestampedRecordOffset {


    /**
     * The position into the current block.
     */
    private final long position;

    /**
     * The number of record read into the current block (since last sync).
     */
    private final long page;

     /**
     * Creates a new {@link ParquetRecordOffset} instance.
      *
     * @param position the current position on th page
     * @param page the current page
     */
    protected ParquetRecordOffset(final long position,
                                  final long page) {
        this(position, page, Time.SYSTEM.milliseconds());
    }


    /**
     * Creates a new {@link TimestampedRecordOffset} instance.
     *
     * @param timestamp the current timestamp.
     */
    private ParquetRecordOffset(final long position,
                                final long page,
                                final long timestamp) {
        super(timestamp);
        this.position = position;
        this.page = page;
    }

    public long position() {
        return position;
    }

    public long records() {
        return page;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectOffset toSourceOffset() {
        return new FileObjectOffset(position, page, timestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParquetRecordOffset)) return false;
        if (!super.equals(o)) return false;
        ParquetRecordOffset that = (ParquetRecordOffset) o;
        return position == that.position &&
                page == that.page;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), position, page);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String
    toString() {
        return "[" +
                ", position=" + position +
                ", page=" + page +
                ", timestamp=" + timestamp() +
                "]";
    }
}
