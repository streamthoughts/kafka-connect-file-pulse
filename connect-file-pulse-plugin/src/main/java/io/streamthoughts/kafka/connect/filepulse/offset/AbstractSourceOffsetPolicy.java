/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.offset;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.source.SourceTaskContext;

public abstract class AbstractSourceOffsetPolicy implements SourceOffsetPolicy {

    private final static String POSITION_OFFSET_FIELD      = "position";
    private final static String POSITION_ROWS_FIELD        = "rows";
    private final static String POSITION_TIMESTAMP_FIELD   = "timestamp";

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<FileObjectOffset> getOffsetFor(final SourceTaskContext context,
                                                   final FileObjectMeta source) {

        final Map<String, Object> partition = toPartitionMap(source);

        final Map<String, Object> offset = context.offsetStorageReader().offset(partition);

        Object offsetBytes = (offset != null) ? offset.get(POSITION_OFFSET_FIELD) : null;
        Object rows        = (offset != null) ? offset.get(POSITION_ROWS_FIELD) : null;
        Object timestamp   = (offset != null) ? offset.get(POSITION_TIMESTAMP_FIELD) : null;

        if (offsetBytes == null || rows == null || timestamp == null) {
            return Optional.empty();
        }

        checkOffsetIsValid(offsetBytes);
        checkRowsIsValid(rows);
        checkTimestampIsValid(timestamp);

        return Optional.of(new FileObjectOffset((Long) offsetBytes, (Long) rows, (Long) timestamp));
    }

    private void checkTimestampIsValid(final Object timestamp) {
        if (!(timestamp instanceof Long)) {
            throw new ConnectFilePulseException("Incorrect type for the last active timestamp");
        }
    }

    private void checkRowsIsValid(final Object rows) {
        if (!(rows instanceof Long)) {
            throw new ConnectFilePulseException("Incorrect type for number of rows");
        }
    }

    private void checkOffsetIsValid(final Object offsetBytes) {
        if (!(offsetBytes instanceof Long)) {
            throw new ConnectFilePulseException("Incorrect type for position bytes");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, ?> toOffsetMap(final FileObjectOffset offset) {
        Objects.requireNonNull(offset, "position can't be null");
        Map<String, Long> map = new HashMap<>();
        map.put(POSITION_OFFSET_FIELD, offset.position());
        map.put(POSITION_ROWS_FIELD, offset.rows());
        map.put(POSITION_TIMESTAMP_FIELD, offset.timestamp());
        return map;
    }
}
