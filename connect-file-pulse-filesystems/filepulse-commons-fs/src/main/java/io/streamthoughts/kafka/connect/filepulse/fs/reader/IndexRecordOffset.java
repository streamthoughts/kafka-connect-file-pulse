/*
 * Copyright 2019-2021 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
