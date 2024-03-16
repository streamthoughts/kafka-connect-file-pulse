/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.converter;

public interface PropertyConverter {

    /**
     * Returns {@code null} which means this is a general purpose converter.
     *
     * @return the array of {@link Class}?
     */
    default Class<?>[] getSpecificTargetClasses() {
        return null;
    }

    /**
     * Checks whether this converter can convert the specified class.
     *
     * @param o         the object to be converted.
     * @param classType the target type.
     * @return          {@code true} if is convertible.
     */
    boolean canConvert(final Object o, final Class<?> classType);

    /**
     * Converts an object to a specified target type.
     *
     * @param o             the object to be converted
     * @param classType     the {@link Class} of expected type.
     * @param <T>           the target type.
     * @return              the converted object.
     *
     * @throws ConversionException if an error occurred while converting object o.
     */
    <T> T convert(final Object o, final Class<T> classType) throws ConversionException;

}
