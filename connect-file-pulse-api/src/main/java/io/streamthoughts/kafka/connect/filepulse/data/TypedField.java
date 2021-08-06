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
        return index == that.index &&
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
