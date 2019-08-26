/*
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SimpleSchema implements Schema {

    private static final Map<Type, Schema> SCHEMAS_KEYED_BY_TYPE = new HashMap<>();

    static final Schema SCHEMA_STRING = new SimpleSchema(Type.STRING);

    static final Schema SCHEMA_INT_64 = new SimpleSchema(Type.LONG);

    static final Schema SCHEMA_INT_16 = new SimpleSchema(Type.SHORT);

    static final Schema SCHEMA_FLOAT_64 = new SimpleSchema(Type.DOUBLE);

    static final Schema SCHEMA_BOOLEAN = new SimpleSchema(Type.BOOLEAN);

    static final Schema SCHEMA_INT_32 = new SimpleSchema(Type.INTEGER);

    static final Schema SCHEMA_FLOAT_32 = new SimpleSchema(Type.DOUBLE);

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

    public Type type() {
        return type;
    }

    public <T> T map(final SchemaMapper<T> mapper) {
        return mapper.map(this);
    }

    @Override
    public <T> T map(final SchemaMapperWithValue<T> mapper, final Object object) {
        return mapper.map(this, object);
    }

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
