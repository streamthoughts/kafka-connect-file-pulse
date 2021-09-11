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
