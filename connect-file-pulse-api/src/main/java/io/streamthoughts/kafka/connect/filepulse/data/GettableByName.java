/*
 * Copyright 2019 StreamThoughts.
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
     * @throws DataException if content {@code field} is not of type MAP.
     */
    <K, V> Map<K, V> getMap(final String field) throws DataException;

    TypedValue get(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a values.
     *
     * @throws DataException if content {@code field} is not of type VALUE.
     */
    TypedStruct getStruct(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a collection.
     *
     * @throws DataException if content {@code field} is not of type ARRAY.
     */
    <T> Collection<T> getArray(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a boolean.
     *
     * @throws DataException if content {@code field} is not of type INT-16.
     */
    Short getShort(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a double.
     *
     * @throws DataException if content {@code field} is not of type BOOLEAN.
     */
    Boolean getBoolean(final String field) throws DataException;

    /**
     * Returns the {@code field} content as an integer.
     *
     * @throws DataException if content {@code field} is not of type INT32.
     */
    Integer getInt(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a long.
     *
     * @throws DataException if content {@code field} is not of type INT64.
     */
    Long getLong(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a float.
     *
     * @throws DataException if content {@code field} is not of type FLOAT.
     */
    Float getFloat(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a double.
     *
     * @throws DataException if content {@code field} is not of type DOUBLE.
     */
    Double getDouble(final String field) throws DataException;

    /**
     * Returns the {@code field} content as a string.
     *
     * @throws DataException if content {@code field} is not of STRING.
     */
    String getString(final String field) throws DataException;

}
