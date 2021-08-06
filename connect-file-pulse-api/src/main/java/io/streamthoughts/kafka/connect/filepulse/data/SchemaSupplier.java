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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public interface SchemaSupplier extends Supplier<Schema> {

    /**
     * Returns a supplier that will infer schema from the specified object.
     *
     * @param object the object to be used to infer schema.
     * @return a new {@link LazySchemaSupplier} instance.
     */
    static SchemaSupplier lazy(final Object object) {
        return new LazySchemaSupplier(object);
    }

    /**
     * Returns a supplier that will return the specified schema.
     *
     * @param schema the {@link Schema} instance to supply
     * @return a new {@link EagerSchemaSupplier} instance.
     */
    static SchemaSupplier eager(final Schema schema) {
        return new EagerSchemaSupplier(schema);
    }

    /**
     * Supplies the {@link Schema} instance.
     *
     * @return the {@link Schema} instance.
     */
    @Override
    Schema get();

    class LazySchemaSupplier implements SchemaSupplier {

        private final Object value;
        private Schema schema;

        /**
         * Creates a new {@link LazySchemaSupplier} instance.
         *
         * @param value the object to be used to infer the schema to returned.
         */
        LazySchemaSupplier(final Object value) {
            this.value = value;
            if (this.value == null) {
                schema = Schema.none();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Schema get() {
            mayInferSchemaFromValue();
            return schema;
        }

        private void mayInferSchemaFromValue() {
            if (schema == null) {
                if (value == null) {
                    schema = SimpleSchema.forType(Type.NULL);
                    return;
                }

                Type type = Type.forClass(value.getClass());
                if (type == null) {
                    throw new DataException("Cannot infer schema for type " + value.getClass());
                }

                if (type.isPrimitive()) {
                    schema = SimpleSchema.forType(type);
                } else if (type == Type.STRUCT) {
                    schema = ((TypedStruct) value).schema();
                } else if (type == Type.MAP) {
                    schema = new LazyMapSchema((Map) value);
                } else if (type == Type.ARRAY) {
                    schema = new LazyArraySchema((List) value);
                } else {
                    throw new DataException("Cannot infer schema for type " + value.getClass());
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            mayInferSchemaFromValue();
            if (this == o) return true;
            if (!(o instanceof LazySchemaSupplier)) return false;
            LazySchemaSupplier that = (LazySchemaSupplier) o;
            return Objects.equals(value, that.value) &&
                    Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            mayInferSchemaFromValue();
            return Objects.hash(value, schema);
        }

        @Override
        public String toString() {
            mayInferSchemaFromValue();
            return schema.toString();
        }
    }

    class EagerSchemaSupplier implements SchemaSupplier {

        private final Schema schema;

        /**
         * Creates a new {@link EagerSchemaSupplier} instance.
         *
         * @param schema the {@link Schema} to be returned.
         */
        EagerSchemaSupplier(final Schema schema) {
            Objects.requireNonNull(schema, "schema cannot be null");
            this.schema = schema;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Schema get() {
            return schema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EagerSchemaSupplier)) return false;
            EagerSchemaSupplier that = (EagerSchemaSupplier) o;
            return Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema);
        }

        @Override
        public String toString() {
            return schema.toString();
        }
    }
}
