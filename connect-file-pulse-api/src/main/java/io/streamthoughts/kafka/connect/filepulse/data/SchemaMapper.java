/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

/**
 * Default interface to map {@link Schema} and a value.
 *
 * @param <T>   target type.
 */
public interface SchemaMapper<T> {

    /**
     * Map the given {@link MapSchema} to the target type T.
     *
     * @param schema        the {@link Schema}.
     * @param optional      {@code true} if the schema should be optional.
     * @return              T
     */
    T map(final MapSchema schema, final boolean optional);

    /**
     * Map the given {@link ArraySchema} to the target type T.
     *
     * @param schema        the {@link Schema}.
     * @param optional      {@code true} if the schema should be optional.
     * @return              T
     */
    T map(final ArraySchema schema, final boolean optional);

    /**
     * Map the given {@link StructSchema} to the target type T.
     *
     * @param schema        the {@link Schema}.
     * @param optional      {@code true} if the schema should be optional.
     * @return              T
     */
    T map(final StructSchema schema, final boolean optional);

    /**
     * Map the given {@link SimpleSchema} to the target type T.
     *
     * @param schema        the {@link Schema}.
     * @param optional      {@code true} if the schema should be optional.
     * @return              T
     */
    T map(final SimpleSchema schema, final boolean optional);
}
