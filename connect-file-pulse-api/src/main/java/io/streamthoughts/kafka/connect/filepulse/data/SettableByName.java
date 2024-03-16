/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Collection of (typed) values that can be retrieved by name.
 *
 * @param <T>   the {@link SettableByName} type.
 */
public interface SettableByName<T extends SettableByName> extends Serializable {

    /**
     * Put the content for the specified field name.
     *
     * @param field the name of the field.
     * @param object the object to put.
     * @param type the object type.
     *
     * @return        this instance.
     */
    T put(final String field, final Type type, final Object object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param value   the object to put.
     * @return        this instance.
     */

    T put(final String field, final TypedStruct value)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param value   the object to put.
     * @param <E>     the element type.
     *
     * @return        this instance.
     */
    <E> T put(final String field, final List<E> value)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param value   the object to put.
     * @param <E>     the element type.
     *
     * @return        this instance.
     */
    <E> T put(final String field, final E[] value)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     * @param <K>     the key type.
     * @param <V>     the value type.
     *
     * @return        this instance.
     */
    <K, V> T put(final String field, final Map<K, V> object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final Boolean object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final Short object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final Integer object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final Long object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final Double object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final Float object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final String object)
            throws DataException;

    /**
     * Put the content for the specified field name.
     *
     * @param field   the name of the field.
     * @param object  the object to put.
     *
     * @return        this instance.
     */
    T put(final String field, final byte[] object)
            throws DataException;
}