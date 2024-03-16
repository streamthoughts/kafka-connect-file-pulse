/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class CyclicSchemaWrapper implements Schema {

    private Schema schema;

    public CyclicSchemaWrapper(final Schema schema) {
        schema(schema);
    }

    public void schema(final Schema schema) {
        this.schema = Objects.requireNonNull(schema, "'schema' should not be null");
    }

    @Override
    public Type type() {
        return schema.type();
    }

    @Override
    public boolean isOptional() {
        return schema.isOptional();
    }

    @Override
    public Object defaultValue() {
        return schema.defaultValue();
    }

    @Override
    public String name() {
        return schema.name();
    }

    @Override
    public Integer version() {
        return schema.version();
    }

    @Override
    public String doc() {
        return schema.doc();
    }

    @Override
    public Map<String, String> parameters() {
        return schema.parameters();
    }

    @Override
    public Schema keySchema() {
        return schema.keySchema();
    }

    @Override
    public Schema valueSchema() {
        return schema.valueSchema();
    }

    @Override
    public List<Field> fields() {
        return schema.fields();
    }

    @Override
    public Field field(String s) {
        return schema.field(s);
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CyclicSchemaWrapper other = (CyclicSchemaWrapper) o;
        return Objects.equals(schema, other.schema);
    }

    @Override
    public String toString() {
        if (schema.name() != null)
            return "Schema{" + schema.name() + ":" + schema.type() + ", " + fields() + "}";
        else
            return "Schema{" + schema.type() + "}";
    }
}