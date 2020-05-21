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

import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface OffsetManager {

    /**
     * Retrieves the position for the specified context and metadata.
     *
     * @param context       the source task context.
     * @param metadata      the source metadata.
     * @return a new {@link SourceOffset} instance.
     */
    Optional<SourceOffset> getOffsetFor(final SourceTaskContext context, final SourceMetadata metadata);


    /**
     * Converts the specified {@link SourceOffset} into connect position map.
     *
     * @param offset  the {@link SourceOffset} to convert.
     * @return an {@link Map} instance.
     */
    Map<String, ?> toOffsetMap(final SourceOffset offset);

    /**
     * Converts the specified {@link SourceMetadata} into connect partition map.
     *
     * @param metadata  the {@link SourceMetadata} to convert.
     * @return an {@link Map} instance.
     */
    Map<String, ?> toPartitionMap(final SourceMetadata metadata);

    default String toPartitionJson(final SourceMetadata metadata) {
        final Map<String, ?> partition = toPartitionMap(metadata);
        return "{" +
                partition.entrySet()
                        .stream()
                        .map(e -> "\"" + e.getKey() + "\":" + "\"" + e.getValue() + "\"")
                        .collect(Collectors.joining(",")) +
                "}";
    }
}


