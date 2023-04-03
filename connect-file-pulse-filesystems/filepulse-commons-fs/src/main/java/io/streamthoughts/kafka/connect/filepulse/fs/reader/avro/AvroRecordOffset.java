/*
 * Copyright 2019-2021 StreamThoughts.
 *
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.avro;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TimestampedRecordOffset;
import java.util.Objects;
import org.apache.kafka.common.utils.Time;

/**
 * Represents the position of a record into an Avro container file.
 */
public class AvroRecordOffset extends TimestampedRecordOffset {

    /**
     * The start position of the current block.
     */
    private final long blockStart;

    /**
     * The position into the current block.
     */
    private final long position;

    /**
     * The number of record read into the current block (since last sync).
     */
    private final long records;

     /**
     * Creates a new {@link AvroRecordOffset} instance.
      *
     * @param blockStart
     * @param position
     * @param records
     */
    protected AvroRecordOffset(final long blockStart,
                               final long position,
                               final long records) {
        this(blockStart, position, records, Time.SYSTEM.milliseconds());
    }


    /**
     * Creates a new {@link TimestampedRecordOffset} instance.
     *
     * @param timestamp the current timestamp.
     */
    private AvroRecordOffset(final long blockStart,
                             final long position,
                             final long records,
                             final long timestamp) {
        super(timestamp);
        this.blockStart = blockStart;
        this.position = position;
        this.records = records;
    }

    public long blockStart() {
        return blockStart;
    }

    public long position() {
        return position;
    }

    public long records() {
        return records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectOffset toSourceOffset() {
        return new FileObjectOffset(blockStart, records, timestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AvroRecordOffset)) return false;
        if (!super.equals(o)) return false;
        AvroRecordOffset that = (AvroRecordOffset) o;
        return blockStart == that.blockStart &&
                position == that.position &&
                records == that.records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), blockStart, position, records);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String
    toString() {
        return "[" +
                "blockStart=" + blockStart +
                ", position=" + position +
                ", records=" + records +
                ", timestamp=" + timestamp() +
                "]";
    }
}
