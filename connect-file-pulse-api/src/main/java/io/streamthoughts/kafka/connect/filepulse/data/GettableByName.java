/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Collection of (typed) values that can be retrieved by name.
 */
public interface GettableByName extends Serializable {


    /**
     * Checks if the specified field exists.
     *
     * @param   field the field to check.
     * @return  {@code null} if field exists.
     */
    boolean has(final String field);

    /**
     * Returns the {@code field} content as a Map.
     *
     * @param field   the field name.
     * @param <K>     the key type
     * @param <V>     the value type.
     * @return        the field as map.
     *
     * @throws DataException if content {@code field} is not of type MAP.
     */

    <K, V> Map<K, V> getMap(final String field) throws DataException;

    TypedValue get(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a values.
     *
     * @param         field the field name.
     * @return        the field as struct.
     *
     * @throws DataException if content {@code field} is not of type VALUE.
     */
    TypedStruct getStruct(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a collection.
     *
     * @param field   the field name.
     * @param <T>     the value type.
     * @return        the field as array.
     *
     * @throws DataException if content {@code field} is not of type ARRAY.
     */
    <T> Collection<T> getArray(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a boolean.
     *
     * @param         field the field name.
     * @return        the field as short.
     *
     * @throws DataException if content {@code field} is not of type INT-16.
     */
    Short getShort(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a double.
     *
     * @param         field the field name.
     * @return        the field as boolean.
     *
     * @throws DataException if content {@code field} is not of type BOOLEAN.
     */
    Boolean getBoolean(final String field) throws DataException;

    /**
     * Returns the {@code field} content as an integer.
     *
     * @param         field the field name.
     * @return        the field as integer.
     *
     * @throws DataException if content {@code field} is not of type INT32.
     */
    Integer getInt(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a long.
     *
     * @param         field the field name.
     * @return        the field as long.
     *
     * @throws DataException if content {@code field} is not of type INT64.
     */
    Long getLong(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a float.
     *
     * @param         field the field name.
     * @return        the field as float.
     *
     * @throws DataException if content {@code field} is not of type FLOAT.
     */
    Float getFloat(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a double.
     *
     * @param         field the field double.
     * @return        the field as float.
     *
     * @throws DataException if content {@code field} is not of type DOUBLE.
     */
    Double getDouble(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a string.
     *
     * @param         field the field string.
     * @return        the field as float.
     *
     * @throws DataException if content {@code field} is not of STRING.
     */
    String getString(final String field) throws DataException;

}
