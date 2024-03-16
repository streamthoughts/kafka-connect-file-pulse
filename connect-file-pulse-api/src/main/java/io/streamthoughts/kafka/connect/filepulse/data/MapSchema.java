/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Map;
import java.util.Objects;

public class MapSchema implements Schema {

    private final Type type;

    private Integer hash;

    private final Schema keySchema = Schema.string();

    private final Schema valueSchema;

    /**
     * Creates a new MapSchema for the specified type.
     *
     * @param valueSchema the {@link Schema} instance.
     */
    MapSchema(final Schema valueSchema) {
        this.type = Type.MAP;
        this.valueSchema = valueSchema;
    }

    public Schema keySchema() {
        return keySchema;
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
        return mapper.map(this, (Map<String, ?>)object, optional);
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

        if (o instanceof MapSchema) {
            final MapSchema that = (MapSchema)o;
            return new MapSchema(this.valueSchema().merge(that.valueSchema()));
        }

        throw new DataException("Cannot merge incompatible schema type " + this.type() + "<>" + o.type());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MapSchema)) return false;
        MapSchema mapSchema = (MapSchema) o;
        return type == mapSchema.type &&
                Objects.equals(hash, mapSchema.hash) &&
                Objects.equals(keySchema, mapSchema.keySchema) &&
                Objects.equals(valueSchema, mapSchema.valueSchema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        if (hash == null) {
            hash = Objects.hash(type, hash, keySchema, valueSchema);
        }
        return hash;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "type=" + type +
                ", keySchema=" + keySchema +
                ", valueSchema=" + valueSchema +
                "]";
    }
}