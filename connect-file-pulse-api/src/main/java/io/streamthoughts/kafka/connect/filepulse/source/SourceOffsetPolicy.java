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
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceTaskContext;

public interface SourceOffsetPolicy extends Configurable {

    /**
     * {@inheritDoc}
     */
    @Override
    default void configure(final Map<String, ?> configs) { }

    /**
     * Retrieves the position for the specified context and metadata.
     *
     * @param context       the source task context.
     * @param source        the source object.
     * @return a new {@link FileObjectOffset} instance.
     */
    Optional<FileObjectOffset> getOffsetFor(final SourceTaskContext context,
                                            final FileObjectMeta source);

    /**
     * Converts the specified {@link FileObjectOffset} into connect position map.
     *
     * @param offset  the {@link FileObjectOffset} to convert.
     * @return an {@link Map} instance.
     */
    Map<String, ?> toOffsetMap(final FileObjectOffset offset);

    /**
     * Builds a new offset based on the given {@link FileObjectMeta} and offset.
     *
     * @param source    the source object.
     * @return          the offset {@link Map}.
     */
    Map<String, Object> toPartitionMap(final FileObjectMeta source);

    /**
     * An helper method to get a {@link FileObjectMeta} as a JSON string.
     *
     * @param source    the source object.
     * @return          a new JSON string.
     */
    default String toPartitionJson(final FileObjectMeta source) {
        final Map<String, ?> partition = toPartitionMap(source);
        return "{" +
                partition.entrySet()
                        .stream()
                        .map(e -> "\"" + e.getKey() + "\":" + "\"" + e.getValue() + "\"")
                        .collect(Collectors.joining(",")) +
                "}";
    }
}
