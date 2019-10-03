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
    boolean canConvert(final Object o, final Class classType);

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
