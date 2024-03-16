/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
