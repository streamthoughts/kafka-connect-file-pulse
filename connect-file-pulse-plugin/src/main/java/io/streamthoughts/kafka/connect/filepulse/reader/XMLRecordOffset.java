/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TimestampedRecordOffset;
import org.apache.kafka.common.utils.Time;

public class XMLRecordOffset extends TimestampedRecordOffset {

    private final long records;

    /**
     * Creates a new {@link XMLRecordOffset} instance.
     *
     * @param records
     */
    public XMLRecordOffset(final long records) {
        this(Time.SYSTEM.milliseconds(), records);
    }

    /**
     * Creates a new {@link XMLRecordOffset} instance.
     *
     * @param timestamp
     * @param records
     */
    private XMLRecordOffset(final long timestamp, final long records) {
        super(timestamp);
        this.records = records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SourceOffset toSourceOffset() {
        return new SourceOffset(records, records, timestamp());
    }
}
