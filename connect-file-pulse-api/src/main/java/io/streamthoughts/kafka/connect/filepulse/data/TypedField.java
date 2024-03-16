/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Objects;

public class TypedField {

    private final Integer index;
    private final Schema schema;
    private final String name;

    /**
     * Creates a new {@link TypedField} instance.
     *
     * @param schema  the field type.
     * @param name    the field name.
     */
    TypedField(final Schema schema,
               final String name) {
        this(null, schema, name);
    }

    /**
     * Creates a new {@link TypedField} instance.
     *
     * @param index   the field index into the {@link TypedStruct}.
     * @param schema  the field type.
     * @param name    the field name.
     */
    TypedField(final Integer index,
               final Schema schema,
               final String name) {
        this.index = index;
        this.schema = schema;
        this.name = name;
    }

    int index() {
        return index;
    }

    public Schema schema() {
        return schema;
    }

    public Type type() {
        return schema.type();
    }

    public String name() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypedField)) return false;
        TypedField that = (TypedField) o;
        return  Objects.equals(index, that.index) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(name, that.name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(index, schema, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "index=" + index +
                ", schema=" + schema +
                ", name=" + name +
                "]";
    }
}
