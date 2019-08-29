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
package io.streamthoughts.kafka.connect.filepulse.offset;

import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum OffsetStrategy {

    FILENAME_HASH("name+hash") {
        @Override
        Map<String, Object> toPartitionMap(final SourceMetadata metadata) {
            return new HashMap<String, Object>() {{
                put(FILENAME_FIELD, metadata.name());
                put(CRC32_FIELD, metadata.hash());
            }};
        }
    },

    FILENAME("name") {
        @Override
        Map<String, Object> toPartitionMap(final SourceMetadata metadata) {
            return new HashMap<String, Object>() {{
                put(FILENAME_FIELD, metadata.name());
            }};
        }
    },

    FILEPATH("path") {
        @Override
        Map<String, Object> toPartitionMap(final SourceMetadata metadata) {
            return Collections.singletonMap(FILEPATH_FIELD, metadata.path());
        }
    };

    private static final String FILEPATH_FIELD = "path";
    private static final String FILENAME_FIELD = "name";
    private static final String CRC32_FIELD    = "hash";

    private final String label;

    /**
     * Creates a new {@link OffsetStrategy} instance.
     * @param label the strategy label.
     */
    OffsetStrategy(final String label) {
        this.label = label;
    }

    abstract Map<String, Object> toPartitionMap(final SourceMetadata metadata);

    public String label() {
        return this.label;
    }

    public static OffsetStrategy getForLabel(final String label) {
        OffsetStrategy strategy = null;
        for (OffsetStrategy s : OffsetStrategy.values()) {
            if (s.label().equals(label.toLowerCase())) {
                strategy = s;
                break;
            }
        }
        return strategy;
    }
}
