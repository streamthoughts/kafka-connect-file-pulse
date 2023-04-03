/*
 * Copyright 2023 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public final class CyclicSchemaWrapper implements Schema {

    private final Schema schema;
    private final boolean optional;

    public CyclicSchemaWrapper(Schema schema) {
        this(schema, schema.isOptional());
    }

    public CyclicSchemaWrapper(Schema schema, boolean optional) {
        this.schema = schema;
        this.optional = optional;
    }

    @Override
    public Type type() {
        return schema.type();
    }

    @Override
    public boolean isOptional() {
        return optional;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CyclicSchemaWrapper other = (CyclicSchemaWrapper) o;
        return Objects.equals(optional, other.optional) && Objects.equals(schema, other.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(optional) + Objects.hashCode(schema);
    }
}
