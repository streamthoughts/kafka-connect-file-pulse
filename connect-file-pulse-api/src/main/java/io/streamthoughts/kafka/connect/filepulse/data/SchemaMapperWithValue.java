/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;

/**
 * Default interface to map {@link io.streamthoughts.kafka.connect.filepulse.data.Schema} and a value.
 *
 * @param <T>   target type.
 */
public interface SchemaMapperWithValue<T> {

    T map(final MapSchema schema, final Map<String, ?> value, final boolean optional);

    T map(final ArraySchema schema, final Collection<?> value, final boolean optional);

    T map(final StructSchema schema, final TypedStruct value, final boolean optional);

    T map(final SimpleSchema schema, final Object value, final boolean optional);

    SchemaAndValue map(final org.apache.kafka.connect.data.Schema schema,
                       final TypedStruct value);
}
