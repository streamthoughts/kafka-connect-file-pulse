/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SimpleSchema implements Schema {

    private static final Map<Type, Schema> SCHEMAS_KEYED_BY_TYPE = new HashMap<>();

    static final Schema SCHEMA_STRING = new SimpleSchema(Type.STRING);

    static final Schema SCHEMA_INT_16 = new SimpleSchema(Type.SHORT);

    static final Schema SCHEMA_INT_32 = new SimpleSchema(Type.INTEGER);

    static final Schema SCHEMA_INT_64 = new SimpleSchema(Type.LONG);

    static final Schema SCHEMA_FLOAT_32 = new SimpleSchema(Type.FLOAT);

    static final Schema SCHEMA_FLOAT_64 = new SimpleSchema(Type.DOUBLE);

    static final Schema SCHEMA_BOOLEAN = new SimpleSchema(Type.BOOLEAN);

    static final Schema SCHEMA_BYTES = new SimpleSchema(Type.BYTES);

    private final Type type;

    private Integer hash;

    static Schema forType(final Type type) {
        Schema schema = SCHEMAS_KEYED_BY_TYPE.get(type);
        if (schema == null) {
            throw new IllegalArgumentException("Cannot resolve simple schema  for type : " + type);
        }
        return schema;
    }

    /**
     * Creates a new {@link SimpleSchema} for the specified type.
     *
     * @param type  the schema type.
     */
    private SimpleSchema(final Type type) {
        Objects.requireNonNull(type, "type cannot be null");
        this.type = type;
        SCHEMAS_KEYED_BY_TYPE.put(type, this);
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
    public <T> T map(final SchemaMapper<T> mapper, final boolean optional) {
        return mapper.map(this, optional);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T map(final SchemaMapperWithValue<T> mapper, final Object object, final boolean optional) {
        return mapper.map(this, object, optional);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema merge(final Schema that) {
        if (this.equals(that)) return this;

        if (this.type() == that.type()) {
            return this;
        }

        if (!that.type().isPrimitive()) {
            // Let's try to reverse the merge:
            if (that.type() == Type.ARRAY)
                return that.merge(this);
            // Else, do throw a DataException.
        }

        if (this.type() == Type.STRING || that.type() == Type.STRING)
            return Schema.string();

        if ((this.type() == Type.LONG && that.type() == Type.INTEGER) ||
            (that.type() == Type.LONG && this.type() == Type.INTEGER)) {
            return Schema.int64();  // return LONG
        }

        if ( (this.type() == Type.DOUBLE && that.type().isNumber()) ||
             (that.type() == Type.DOUBLE && this.type().isNumber())) {
            return Schema.float64(); // return DOUBLE
        }

        throw new DataException("Cannot merge incompatible schema type " + this.type() + "<>" + that.type());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleSchema)) return false;
        SimpleSchema schema = (SimpleSchema) o;
        return type == schema.type;
    }

    @Override
    public int hashCode() {
        if (hash == null) {
            hash = Objects.hash(type);
        }
        return hash;
    }

    @Override
    public String toString() {
        return "[" +
                "type=" + type +
                ']';
    }
}
