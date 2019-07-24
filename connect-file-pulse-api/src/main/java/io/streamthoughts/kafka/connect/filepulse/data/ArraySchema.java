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

import java.util.Collection;
import java.util.Objects;

public class ArraySchema implements Schema {

    private final Type type;
    private final Schema valueSchema;

    private Integer hash;

    /**
     * Creates a new MapSchema for the specified type.
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
    public <T> T map(final SchemaMapper<T> mapper) {
        return mapper.map(this);
    }

    @Override
    public <T> T map(final SchemaMapperWithValue<T> mapper, final Object object) {
        return mapper.map(this, (Collection)object);
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
