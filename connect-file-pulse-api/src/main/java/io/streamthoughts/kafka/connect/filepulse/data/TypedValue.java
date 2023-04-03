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

import io.streamthoughts.kafka.connect.filepulse.data.internal.TypeConverter;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Class which is used to pair an object with its corresponding {@link Schema}.
 * TypeValue supports dynamic type conversion.
 */
public class TypedValue implements GettableByType {

    public static TypedValue none() {
        return new TypedValue(Schema.none(), null);
    }

    private final Object value;
    private final SchemaSupplier schema;

    public static TypedValue parse(final String text) {
        final String trimmed = text.trim();
        if (TypeConverter.isIntegerNumber(trimmed)) {
            // check if string fit within `long`
            if (trimmed.length() <= 19 && TypeConverter.isInLongRange(trimmed)) {
                return TypedValue.int64(Long.parseLong(trimmed));
            }
            // return directly the string type because a text containing only numbers
            // could be interpreted as Double, which could result in a loss of precision
            return TypedValue.string(text);
        }
        if (TypeConverter.isDoubleNumber(trimmed)) {
            return TypedValue.float64(Double.parseDouble(trimmed));
        }
        if (TypeConverter.isBooleanString(trimmed)) {
            return TypedValue.bool(Boolean.parseBoolean(trimmed));
        }

        return TypedValue.string(text);
    }

    public static TypedValue of(final Object value, final Schema schema) {
        return new TypedValue(schema, value);
    }

    public static TypedValue of(final Object value, final Type type) {
        return new TypedValue(Schema.of(type), value);
    }

    public static TypedValue any(final Object value) {
        Object supportedValue = value;
        if (value != null) {
            Type supportedType = Type.forClass(supportedValue.getClass());
            if (supportedType == null) {
                supportedValue = TypeConverter.getString(value);
            }
        }
        return new TypedValue(supportedValue);
    }

    /**
     * Creates a new {@link TypedValue} of string.
     *
     * @param value the string value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue string(final String value) {
        return new TypedValue(Schema.string(), value);
    }

    /**
     * Creates a new {@link TypedValue} of boolean.
     *
     * @param value the boolean value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue bool(final Boolean value) {
        return new TypedValue(Schema.bool(), value);
    }

    /**
     * Creates a new {@link TypedValue} of short.
     *
     * @param value the short value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue int16(final Short value) {
        return new TypedValue(Schema.int16(), value);
    }

    /**
     * Creates a new {@link TypedValue} of type int.
     *
     * @param value the integer value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue int32(final Integer value) {
        return new TypedValue(Schema.int32(), value);
    }

    /**
     * Creates a new {@link TypedValue} of long.
     *
     * @param value the long value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue int64(final Long value) {
        return new TypedValue(Schema.int64(), value);
    }

    /**
     * Creates a new {@link TypedValue} of double.
     *
     * @param value the double value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue float64(final Double value) {
        return new TypedValue(Schema.float64(), value);
    }

    /**
     * Creates a new {@link TypedValue} of double.
     *
     * @param value the double value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue float32(final Float value) {
        return new TypedValue(Schema.float32(), value);
    }

    /**
     * Creates a new {@link TypedValue} of double.
     *
     * @param value the struct value.
     * @return      the new {@link TypedValue} instance.
     */
    public static TypedValue struct(final TypedStruct value) {
        return new TypedValue(
            new StructSchema(value.schema()),
            value
        );
    }

    /**
     * Creates a new {@link TypedValue} of map.
     *
     * @param value     the map value.
     * @param valueType the map value-type.
     * @return          the new {@link TypedValue} instance.
     */
    public static TypedValue map(final Map<String ,?> value, final Type valueType) {
        return new TypedValue(Schema.map(value, Schema.of(valueType)), value);
    }

    /**
     * Creates a new {@link TypedValue} of array.
     *
     * @param value       the array value.
     * @param valueSchema the array value-schema.
     * @return            the new {@link TypedValue} instance.
     */
    public static TypedValue array(final Collection<?> value, final Schema valueSchema) {
        return new TypedValue(Schema.array(value, valueSchema), value);
    }

    /**
     * Creates a new {@link TypedValue} of array.
     *
     * @param value     the array value.
     * @param valueType the array value-type.
     * @return          the new {@link TypedValue} instance.
     */
    public static TypedValue array(final Collection<?> value, final Type valueType) {
        return array(value, Schema.of(valueType));
    }

    /**
     * Creates a new {@link TypedValue} of bytes.
     *
     * @param value     the bytes array.
     * @return          the new {@link TypedValue} instance.
     */
    public static TypedValue bytes(final byte[] value) {
        return new TypedValue(Schema.bytes(), value);
    }

    /**
     * Creates a new {@link TypedValue} of bytes.
     *
     * @param value     the bytes array.
     * @return          the new {@link TypedValue} instance.
     */
    public static TypedValue bytes(final ByteBuffer value) {
        return new TypedValue(Schema.bytes(), value.array());
    }


    /**
     * Creates a new {@link TypedValue}
     *
     * @param value the object value.
     */
    private TypedValue(final Object value) {
        this(SchemaSupplier.lazy(value), value);
    }

    /**
     * Creates a new {@link TypedValue}
     *
     * @param value the object value.
     */
    private TypedValue(final Schema schema, final Object value) {
        this(SchemaSupplier.eager(schema), value);
    }

    /**
     * Creates a new {@link TypedValue}
     *
     * @param value the object value.
     */
    private TypedValue(final SchemaSupplier schema, final Object value) {
        Objects.requireNonNull(schema, "schema cannot null");
        this.value = value;
        this.schema = schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap() throws DataException {
        return (Map<K, V>) value;
    }

    @SuppressWarnings("unchecked")
    public <T> T value() {
        return (T) value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<T> getArray() throws DataException {
        return TypeConverter.getArray(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean getBool() throws DataException {
        return TypeConverter.getBool(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getInt() throws DataException {
        return TypeConverter.getInt(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Short getShort() throws DataException {
        return TypeConverter.getShort(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getLong() throws DataException {
        return TypeConverter.getLong(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float getFloat() throws DataException {
        return TypeConverter.getFloat(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getDouble() throws DataException {
        return TypeConverter.getDouble(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getDate() throws DataException {
        return TypeConverter.getDate(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString() throws DataException {
        return TypeConverter.getString(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getBytes() throws DataException {
        return TypeConverter.getBytes(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedStruct getStruct() throws DataException {
        return (TypedStruct) value;
    }

    /**
     * Returns the type for this type-value.
     *
     * @return the value type.
     */
    public Type type() {
        return schema.get().type();
    }

    /**
     * Returns the schema for this type-value.
     *
     * @return the value schema.
     */
    public Schema schema() {
        return schema.get();
    }

    public boolean isNull() {
        return value == null;
    }

    public boolean isNotNull() {
        return value != null;
    }

    public boolean isEmpty() {
        final Type type = schema.get().type();
        if (Type.STRING == type) {
            return getString().isEmpty();
        }

        if (Type.MAP == type) {
            return getMap().isEmpty();
        }

        if (Type.ARRAY == type) {
            return getArray().isEmpty();
        }

        if (Type.STRUCT == type) {
            return getStruct().schema().fields().isEmpty();
        }

        throw new DataException("Cannot check empty-value on non-string, non-array, non-struct, and non-map type");
    }

    public TypedValue as(final Type type) {
        Object converted = isNull() ? null : type.convert(value);
        return TypedValue.of(converted, type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypedValue)) return false;
        TypedValue typeValue = (TypedValue) o;
        return Objects.equals(value, typeValue.value) &&
                Objects.equals(schema, typeValue.schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(value, schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "value=" + value +
                ", schema =" + schema +
                ']';
    }
}
