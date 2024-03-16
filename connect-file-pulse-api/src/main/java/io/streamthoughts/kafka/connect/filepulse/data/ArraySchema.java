/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Collection;
import java.util.Objects;

public class ArraySchema implements Schema {

    private final Type type;
    private final Schema valueSchema;

    private Integer hash;

    /**
     * Creates a new ArraySchema for the specified type.
     *
     * @param valueSchema the {@link Schema} instance.
     */
    ArraySchema(final Schema valueSchema) {
        this.type = Type.ARRAY;
        this.valueSchema = valueSchema;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type type() {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T map(SchemaMapper<T> mapper, boolean optional) {
        return mapper.map(this, optional);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T map(final SchemaMapperWithValue<T> mapper, final Object object, final boolean optional) {
        return mapper.map(this, (Collection)object, optional);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isResolvable() {
        return valueSchema.isResolvable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema merge(final Schema o) {
        if (this.equals(o)) return this;

        if (o instanceof ArraySchema) {
            final ArraySchema that = (ArraySchema)o;
            if (!this.isResolvable()) {
                return that;
            }

            if (!that.isResolvable()) {
                return this;
            }

            return Schema.array(this.valueSchema().merge(that.valueSchema()));
        }

        return Schema.array(this.valueSchema().merge(o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArraySchema)) return false;
        ArraySchema that = (ArraySchema) o;
        return type == that.type &&
                Objects.equals(valueSchema, that.valueSchema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        if (hash == null) {
            hash = Objects.hash(type, valueSchema);
        }
        return hash;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "type=" + type() +
                ", valueSchema=" + valueSchema +
                "]";
    }
}
