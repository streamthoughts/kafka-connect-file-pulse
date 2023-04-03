/*
 * Copyright 2021 StreamThoughts.
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