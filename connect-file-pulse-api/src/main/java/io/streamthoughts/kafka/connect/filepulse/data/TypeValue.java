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
import java.util.Date;
import java.util.Map;
import java.util.Objects;

public class TypeValue implements GettableByType {

    private final Object value;
    private final Type type;

    public static TypeValue of(final Object value) {
        Objects.requireNonNull(value, "value can't be null");
        return new TypeValue(value, null);
    }

    public static TypeValue of(final Object value, final Type type) {
        Objects.requireNonNull(value, "value can't be null");
        return new TypeValue(value, type);
    }


    /**
     * Creates a new {@link TypeValue}
     *
     * @param value the object value.
     */
    private TypeValue(final Object value, final Type type) {
        this.value = value;
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap() throws DataException {
        return (Map<K, V>) value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get() throws IllegalArgumentException {
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

    public Type type() {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return value.toString();
    }
}
