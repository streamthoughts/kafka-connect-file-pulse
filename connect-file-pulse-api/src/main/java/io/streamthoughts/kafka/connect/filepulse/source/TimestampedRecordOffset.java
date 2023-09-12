/*
 * Copyright 2019-2020 StreamThoughts.
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
