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

import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_BOOLEAN;
import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_BYTES;
import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_FLOAT_32;
import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_FLOAT_64;
import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_INT_16;
import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_INT_32;
import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_INT_64;
import static io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema.SCHEMA_STRING;

import java.util.Collection;
import java.util.Map;

public interface Schema {

    static Schema none() {
        return EmptySchema.INSTANCE;
    }

    static Schema of(final Type type) {
        if (type.isPrimitive()) {
            return SimpleSchema.forType(type);
        }
        return null;
    }

    /**
     * Gets a schema for type STRING.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema string() {
        return SCHEMA_STRING;
    }

    /**
     * Gets a schema for type INT_64.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema int64() {
        return SCHEMA_INT_64;
    }

    /**
     * Gets a schema for type INT_16.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema int16() {
        return SCHEMA_INT_16;
    }

    /**
     * Gets a schema for type INT_32.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema int32() {
        return SCHEMA_INT_32;
    }

    /**
     * Gets a schema for type FLOAT_32.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema float32() {
        return SCHEMA_FLOAT_32;
    }

    /**
     * Gets a schema for type FLOAT_64.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema float64() {
        return SCHEMA_FLOAT_64;
    }

    /**
     * Gets a schema for type BOOLEAN.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema bool() {
        return SCHEMA_BOOLEAN;
    }

    /**
     * Gets a schema for type array of BYTES.
     *
     * @return  the {@link Schema} instance.
     */
    static Schema bytes() {
        return SCHEMA_BYTES;
    }

    /**
     * Gets the schema for type STRUCT.
     *
     * @return  the {@link Schema} instance.
     */
    static StructSchema struct() {
        return new StructSchema();
    }

    /**
     * Gets the schema for type MAP.
     *
     * @param valueSchema the {@link Schema} instance.
     *
     * @return  the {@link Schema} instance.
     */
    static MapSchema map(final Map<String ,?> value, final Schema valueSchema) {
        return valueSchema == null ? new LazyMapSchema(value) : new MapSchema(valueSchema);
    }

    /**
     * Gets the schema for type ARRAY.
     *
     * @param valueSchema the {@link Schema} instance.
     *
     * @return  the {@link Schema} instance.
     */
    static ArraySchema array(final Schema valueSchema) {
        return new ArraySchema(valueSchema);
    }

    /**
     * Gets the schema for type ARRAY.
     *
     * @param valueSchema the {@link Schema} instance.
     *
     * @return  the {@link Schema} instance.
     */
    static ArraySchema array(final Collection<?> value, final Schema valueSchema) {
        return valueSchema == null ? new LazyArraySchema(value) : new ArraySchema(valueSchema);
    }

    /**
     * Returns the {@link Type} for this schema.
     * @return the schema {@link Type}.
     */
    Type type() ;

    /**
     * Checks whether this schema is resolvable.
     *
     * @see LazyArraySchema
     * @see LazyMapSchema
     * @return  {@code true}.
     */
    default boolean isResolvable() {
        return true;
    }

    /**
     * Maps this schema into a new type T.
     *
     * @param mapper    the {@link SchemaMapper} to be used.
     * @param <T>       the new type.
     * @return          the schema mapped to T.
     */
    <T> T map(final SchemaMapper<T> mapper, final boolean optional);

    <T> T map(final SchemaMapperWithValue<T> mapper,
                      final Object object,
                      final boolean optional);

    /**
     * Merges this schema with the given one.
     *
     * @param that  the schema to merge.
     * @return      a new {@link Schema}.
     */
    default Schema merge(final Schema that) {
        if (this.equals(that)) return this;

        if (this.type() == that.type()) {
            return this;
        }

        throw new DataException("Cannot merge incompatible schema type " + this.type() + "<>" + that.type());
    }

    class EmptySchema implements Schema {

        static EmptySchema INSTANCE = new EmptySchema();

        /**
         * {@inheritDoc}
         */
        @Override
        public Type type() {
            return Type.NULL;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> T map(final SchemaMapper<T> mapper, final boolean optional) {
            throw new UnsupportedOperationException("this method is not supported for schema of type : " + type());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> T map(final SchemaMapperWithValue<T> mapper, final Object object, final boolean optional) {
            throw new UnsupportedOperationException("this method is not supported for schema of type : " + type());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "[type="+Type.NULL+"]";
        }
    }
}
