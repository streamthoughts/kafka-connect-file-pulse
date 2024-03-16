/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public interface GettableByType {

    /**
     * Returns the content as a Map.
     *
     * @param <K>   type of key.
     * @param <V>   type of value.
     * @return {@code Map} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Map}.
     */
    <K, V> Map<K, V> getMap() throws DataException;

    /**
     * Returns this value content as a collection.
     *
     * @param <T> the object-type
     * @return {@code Collection} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Collection}.
     */
    <T> Collection<T> getArray() throws DataException;

    /**
     * Returns this value content as a boolean.
     *
     * @return {@code Boolean} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Boolean}.
     */
    Boolean getBool() throws DataException;

    /**
     * Returns this value content as an integer.
     *
     * @return {@code Integer} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Integer}.
     */
    Integer getInt() throws DataException;

    /**
     * Returns this value content as a short.
     *
     * @return {@code Short} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Short}.
     */
    Short getShort() throws DataException;

    /**
     * Returns this value content as a long.
     *
     * @return {@code Long} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Long}.
     */
    Long getLong() throws DataException;

    /**
     * Returns this value content as a float.
     *
     * @return {@code Float} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Float}.
     */
    Float getFloat() throws DataException;

    /**
     * Returns this value content as a double.
     *
     * @return {@code Double} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Double}.
     */
    Double getDouble() throws DataException;

    /**
     * Returns this value as a date.
     *
     * @return {@code Date} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link Date}.
     */
    Date getDate() throws DataException;

    /**
     * Returns this content as a string.
     *
     * @return {@code string} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link String}.
     */
    String getString() throws DataException;

    /**
     * Returns this content as a struct.
     *
     * @return {@code TypedStruct} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link TypedStruct}.
     */
    TypedStruct getStruct() throws DataException;

    /**
     * Returns this content as a struct.
     *
     * @return {@code bytes} value.
     * @throws DataException if <code>this</code> cannot be convert to {@link TypedStruct}.
     */
    byte[] getBytes() throws DataException;
}
