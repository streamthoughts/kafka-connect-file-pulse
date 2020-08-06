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
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class ComposeOffsetStrategy implements OffsetStrategy {

    private static final String FILEPATH_FIELD = "path";
    private static final String FILENAME_FIELD = "name";
    private static final String INODE_FIELD    = "inode";
    private static final String CRC32_FIELD    = "hash";
    private static final String MODIFIED_FIELD = "lastmodified";

    private static final Map<String, GenericOffsetStrategy> STRATEGIES = new HashMap<>();

    static {
        STRATEGIES.put(FILENAME_FIELD, new GenericOffsetStrategy(FILENAME_FIELD,1, SourceMetadata::name));
        STRATEGIES.put(FILEPATH_FIELD, new GenericOffsetStrategy(FILEPATH_FIELD, 2, SourceMetadata::path));
        STRATEGIES.put(CRC32_FIELD, new GenericOffsetStrategy(CRC32_FIELD, 3, SourceMetadata::hash));
        STRATEGIES.put(MODIFIED_FIELD, new GenericOffsetStrategy(MODIFIED_FIELD, 4, SourceMetadata::lastModified));
        STRATEGIES.put(INODE_FIELD, new GenericOffsetStrategy(INODE_FIELD, 5, metadata -> {
            if (metadata.inode() == null) {
                throw new ConnectFilePulseException(
                        "Unix-inode is not supported. " +
                                "Consider configuring a different 'offset-strategy' (i.e: path, name, hash, name+hash)");
            }
            return metadata.inode();
        }));
    }

    private final String offsetStrategyString;

    private final List<GenericOffsetStrategy> strategies;

    public ComposeOffsetStrategy(final String offsetStrategyString) {
        this.offsetStrategyString = Objects.requireNonNull(offsetStrategyString);
        this.strategies = new ArrayList<>();
        for (String label : offsetStrategyString.split("\\+")) {
            final GenericOffsetStrategy strategy = STRATEGIES.get(label.toLowerCase());
            if (strategy == null) {
                throw new IllegalArgumentException("Unknown offset strategy for name '" + label + "'");
            }
            strategies.add(strategy);
        }
        strategies.sort(Comparator.comparingInt(o -> o.priority));
        if (strategies.isEmpty()) {
            throw new IllegalArgumentException("Cannot build empty 'offset.strategy'");
        }
    }

    @Override
    public Map<String, Object> toPartitionMap(final SourceMetadata metadata) {
        Map<String, Object> offset = new LinkedHashMap<>();
        for (GenericOffsetStrategy strategy : strategies) {
            strategy.addAttributeToPartitionMap(metadata, offset);
        }
        return offset;
    }


    private static class GenericOffsetStrategy {

        final String name;
        final Function<SourceMetadata, Object> offsetFunction;
        final int priority;

        GenericOffsetStrategy(final String name,
                              final int priority,
                              final Function<SourceMetadata, Object> offsetFunction) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
            this.offsetFunction = Objects.requireNonNull(offsetFunction, "offsetFunction cannot be null");
            this.priority = priority;
        }

        void addAttributeToPartitionMap(final SourceMetadata metadata, final Map<String, Object> offset) {
            offset.put(name, offsetFunction.apply(metadata));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "OffsetStrategy[" + offsetStrategyString + "]";
    }
}
