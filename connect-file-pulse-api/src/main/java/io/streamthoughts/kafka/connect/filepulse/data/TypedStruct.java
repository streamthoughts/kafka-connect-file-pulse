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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TypedStruct implements GettableByName, SettableByName<TypedStruct>, Iterable<TypedField> {

    private final StructSchema schema;
    private final List<Object> values;

    /**
     * Static helper that can be used to create a new {@link TypedStruct} instance.
     *
     * @return  the type-struct instance.
     */
    public static TypedStruct create() {
        return new TypedStruct();
    }

    /**
     * Static helper that can be used to create a new {@link TypedStruct} instance with the given name.
     *
     * @param name the name of {@link Schema} for this struct.
     *
     * @return     the type-struct instance.
     */
    public static TypedStruct create(final String name) {
        return create(Schema.struct().name(name));
    }

    /**
     * Static helper that can be used to create a new {@link TypedStruct} instance with the given schema.
     *
     * @param schema    the {@link StructSchema} instance.
     * @return          the type-struct instance.
     */
    private static TypedStruct create(final StructSchema schema) {
        return new TypedStruct(schema);
    }

    /**
     * Creates a new {@link TypedStruct} instance.
     */
    private TypedStruct() {
        this(Schema.struct());
    }

    /**
     * Creates a new {@link TypedStruct} instance.
     */
    private TypedStruct(final StructSchema schema) {
        this.schema = Objects.requireNonNull(schema, "schema cannot be null");
        this.values = new LinkedList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final Short value) {
        return put(field, Schema.int16(), field);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final String value) {
        return put(field, Schema.string(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final Integer value) {
        return put(field, Schema.int32(), field);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final Long value) {
        return put(field, Schema.int64(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final Double value) {
        return put(field, Schema.float64(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final Float value) {
        return put(field, Schema.float32(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final List value) {
        return put(field, new LazyArraySchema(value), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("uncheckec")
    public TypedStruct put(final String field, final Map value) {
        return put(field, new LazyMapSchema(value), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final Boolean value) {
        return put(field, TypedValue.bool(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final TypedStruct value) {
        return put(field, value.schema(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> TypedStruct put(final String field, final E[] value) throws DataException {
        return put(field, Arrays.asList(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final byte[] value) {
        return put(field, Schema.bytes(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct put(final String field, final Type type, final Object value) {
        return put(field, Schema.of(type), value);
    }

    public TypedStruct put(final TypedField field, final TypedValue typed) {
        Objects.requireNonNull(field, "field can't be null");
        return put(field.name(), typed.schema(), typed.value());
    }

    public TypedStruct put(final String field, final TypedValue typed) {
        return put(field, typed.schema(), typed.value());
    }

    public TypedStruct put(final String fieldName,
                           final Schema fieldSchema,
                           final Object fieldValue) {
        if (!has(fieldName)) {
            schema.field(fieldName, fieldSchema);
            values.add(fieldValue);
        } else {
            int index = schema.indexOf(fieldName);
            schema.set(fieldName, fieldSchema); // handle case where fieldName's fieldSchema is changed.
            values.set(index, fieldValue);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean has(final String name) {
        return schema.field(name) != null;
    }

    public TypedValue get(final TypedField field) {
        Objects.requireNonNull(field, "field cannot be null");
        return get(field.name());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue get(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        TypedField field = lookupField(name);
        Object o = values.get(field.index());
        return TypedValue.of(o, field.schema());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(final String field) {
        return getCheckedType(field, Type.STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Short getShort(final String field) {
        return getCheckedType(field, Type.SHORT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean getBoolean(final String field) throws DataException {
        return getCheckedType(field, Type.BOOLEAN);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getInt(final String field) {
        return getCheckedType(field, Type.INTEGER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getLong(final String field) {
        return getCheckedType(field, Type.LONG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float getFloat(final String field) throws DataException {
        return getCheckedType(field, Type.FLOAT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getDouble(final String field) {
        return getCheckedType(field, Type.DOUBLE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> getArray(final String field) {
        return getCheckedType(field, Type.ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct getStruct(final String field) {
        return getCheckedType(field, Type.STRUCT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Map<K, V> getMap(final String field) throws DataException {
        return getCheckedType(field, Type.MAP);
    }


    /**
     * Renames the field present to the given path.
     *
     * @param path      the path of the field.
     * @param newField  the new field name.
     * @return          return this.
     */
    public TypedStruct rename(final String path, final String newField) {
        if (has(path)) {
            schema.rename(path, newField);
            return this;
        }

        if (isDotPropertyAccessPath(path)) {
            String[] split = path.split("\\.", 2);
            if (has(split[0])) {
                TypedValue child = get(split[0]);
                if (child.schema().type() == Type.STRUCT) {
                    return child.getStruct().rename(split[1], newField);
                }
            }
        }
        return this;
    }

    /**
     * Removes the field present to the given path.
     * This method will remove any empty struct field that result from the suppression of the specified field.
     *
     * @param path      the path of the field.
     * @return          return the removed value or {@code null} if no object value exist for the given path.
     */
    public TypedValue remove(final String path) {
        if (has(path)) {
            final TypedValue value = get(path);
            TypedField removed = schema.remove(path);
            if (removed != null) values.remove(removed.index());
            return value;
        }

        if (isDotPropertyAccessPath(path)) {
            final String[] split = path.split("\\.", 2);
            if (has(split[0])) {
                final TypedValue child = get(split[0]);
                if (child.schema().type() == Type.STRUCT) {
                    final TypedStruct childStruct = child.getStruct();
                    final TypedValue removed = childStruct.remove(split[1]);
                    if (removed != null && childStruct.values.isEmpty()) {
                        remove(split[0]);
                    }
                    return removed;
                }
            }
        }
        return null;
    }

    /**
     * Checks if a field exist for the given path.
     *
     * @param path  the path to check.
     * @return      {@code true} if the path exists.
     */
    public boolean exists(final String path) {
        Objects.requireNonNull(path, "path cannot be null");
        return find(path) != null;
    }

    /**
     * Finds the value for the given path.
     *
     * @param path  the path.
     * @return      the value or {@code null} if no value exists.
     */
    public TypedValue find(final String path) {
        Objects.requireNonNull(path, "path cannot be null");
        if (has(path)) return get(path);

        if (isDotPropertyAccessPath(path)) {
            String[] split = path.split("\\.", 2);
            if (has(split[0])) {
                TypedValue child = get(split[0]);
                if (child.schema().type() == Type.STRUCT) {
                    return child.getStruct().find(split[1]);
                }
            }
        }
        return null;
    }

    /**
     * Inserts the given object value to the given path.
     *
     * @param path      the object path.
     * @param value     the object value.
     * @return          return this?
     */
    public TypedStruct insert(final String path, final Object value) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Cannot insert value given null or empty path");
        }
        doInsert(path, (value instanceof TypedValue) ? (TypedValue)value : TypedValue.any(value));
        return this;
    }

    private void doInsert(final String path, final TypedValue value) {
        if (isDotPropertyAccessPath(path)) {
            String[] split = path.split("\\.", 2);
            final String field = split[0];
            final String remaining = split[1];
            TypedStruct child;
            if (has(field)) {
                child = getStruct(field);
            } else {
                child = new TypedStruct();
                put(field, child);
            }
            child.doInsert(remaining, value);
        } else {
            put(path, value);
        }
    }

    private static boolean isDotPropertyAccessPath(final String name) {
        return name.contains(".");
    }

    public TypedValue first(final String fieldName) {
        TypedField field = field(fieldName);

        if (field.type() == Type.ARRAY) {
            List<Object> array = getArray(fieldName);
            if (!array.isEmpty()) {
                return TypedValue.any(array.get(0));
            }
        }

        return get(fieldName);
    }

    public StructSchema schema() {
        return schema;
    }

    public TypedField field(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return lookupField(name);
    }

    private TypedField lookupField(final String name) {
        TypedField field = schema.field(name);
        if (field == null) {
            throw new DataException(name + " is not a valid field name");
        }
        return field;
    }

    private<T> T getCheckedType(final String name, final Type type) {
        TypedValue typed = get(name);
        if (typed.type() == type) {
            return typed.value();
        } else {
            throw new DataException(
                "Field '" + name + "' is not of type " + type + ", actual type is " + typed.type());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypedStruct)) return false;
        TypedStruct that = (TypedStruct) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(values, that.values);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(schema, values);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return StreamSupport.stream(schema.spliterator(), false)
            .map( field -> {
                TypedValue value = get(field);
                return
                    "name: " + field.name()
                    + ", type: " +  value.schema().type()
                    + ", value: " +  value.value();
            })
            .collect(Collectors.joining(", ", "[", "]"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<TypedField> iterator() {
        return schema.iterator();
    }
}
