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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StructSchema implements Schema, Iterable<TypedField> {

    private final Type type;

    private final Map<String, TypedField> fields;

    private Integer hash;

    /**
     * Creates a new {@link StructSchema} instance.
     */
    public StructSchema() {
        this(Collections.emptyList());
    }

    /**
     * Creates a new {@link StructSchema} instance.
     */
    public StructSchema(final Collection<TypedField> fields) {
        this.type = Type.STRUCT;
        this.fields = new LinkedHashMap<>();
        fields.forEach(field -> this.fields.put(field.name(), field));

    }

    public StructSchema field(final String fieldName, final Schema fieldSchema) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        if (null == fieldSchema) {
            throw new DataException("fieldSchema for field " + fieldName + " cannot be null.");
        }
        if (fields.containsKey(fieldName)) {
            throw new DataException("Cannot create field because of field name duplication " + fieldName);
        }
        fields.put(fieldName, new TypedField(fields.size(), fieldSchema, fieldName));
        return this;
    }

    public int indexOf(final String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        return fields.containsKey(fieldName) ? fields.get(fieldName).index() : -1;
    }

    public TypedField field(final String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        return fields.get(fieldName);
    }

    public List<TypedField> fields() {
        return new ArrayList<>(fields.values());
    }

    void set(final String fieldName, final Schema fieldSchema) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new DataException("fieldName cannot be null.");
        }
        if (null == fieldSchema) {
            throw new DataException("fieldSchema for field " + fieldName + " cannot be null.");
        }
        TypedField field = field(fieldName);
        if (field == null) {
            throw new DataException("Cannot set field because of field do not exist " + fieldName);
        }
        fields.put(fieldName, new TypedField(field.index(), fieldSchema, fieldName));
    }

    void rename(final String fieldName, final String newField) {
        TypedField tf = field(fieldName);
        if (fieldName == null) {
            throw new DataException("Cannot rename field because of field do not exist " + fieldName);
        }
        fields.remove(fieldName);
        fields.put(newField, new TypedField(tf.index(), tf.schema(), newField));
    }

    void remove(final String fieldName) {
        TypedField tf = field(fieldName);
        if (tf != null) {
            fields.remove(tf.name());
            fields.replaceAll((k, v) -> {
                if (v.index() > tf.index()) {
                    return new TypedField(v.index() - 1, tf.schema(), tf.name());
                }
                return v;
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<TypedField> iterator() {
        return Collections.unmodifiableCollection(fields.values()).iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type type() {
        return this.type;
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
        return mapper.map(this, (TypedStruct)object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StructSchema)) return false;
        StructSchema that = (StructSchema) o;
        return type == that.type &&
                Objects.equals(fields, that.fields);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        if (hash == null) {
            hash = Objects.hash(type, fields);
        }
        return hash;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "fields=" + fields +
                ']';
    }
}
