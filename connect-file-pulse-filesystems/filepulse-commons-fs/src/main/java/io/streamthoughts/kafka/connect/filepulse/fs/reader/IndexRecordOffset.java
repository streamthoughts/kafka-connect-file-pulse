/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TimestampedRecordOffset;
import org.apache.kafka.common.utils.Time;

/**
 * Represents the position of a record into a file based on an index.
 */
public class IndexRecordOffset extends TimestampedRecordOffset {

    private final long records;

    /**
     * Creates a new {@link IndexRecordOffset} instance.
     *
     * @param records   the record offset.
     */
    public IndexRecordOffset(final long records) {
        this(Time.SYSTEM.milliseconds(), records);
    }

    /**
     * Creates a new {@link IndexRecordOffset} instance.
     *
     * @param timestamp the timestamp attached to this offset.
     * @param records   the record offset.
     */
    private IndexRecordOffset(final long timestamp, final long records) {
        super(timestamp);
        this.records = records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectOffset toSourceOffset() {
        return new FileObjectOffset(records, records, timestamp());
    }
}
