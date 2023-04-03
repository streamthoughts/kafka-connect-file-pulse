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
package io.streamthoughts.kafka.connect.filepulse.expression.converter;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default class to resolve {@link PropertyConverter} for a given object and target {@link Class} type.
 */
public class Converters {

    /**
     * Converts an object to a specific target type using a specified list of {@link PropertyConverter}.
     *
     * @param converters the list of {@link PropertyConverter} which can be used.
     * @param object     the object to be converted.
     * @param classType  the {@link Class} of expected type.
     * @param <T>        the target type.
     * @return the converted object of type T.
     * @throws ConversionException if an error occurred while converting object o.
     */
    @SuppressWarnings("unchecked")
    public static <T> T converts(final List<PropertyConverter> converters,
                                 final Object object,
                                 final Class<T> classType) throws ConversionException {

        final List<PropertyConverter> resolved = resolves(converters, object, classType);

        if (resolved.isEmpty()) {
            if (object == null) return null;

            final Object objectToConvert = object.getClass().equals(TypedValue.class) ?
                    ((TypedValue) object).value() : object;

            if (classType.isAssignableFrom(objectToConvert.getClass())) {
                return (T) objectToConvert;
            }

            throw new ConversionException(
                    String.format(
                            "Cannot found any property converter for type '%s' and object %s",
                            classType.getCanonicalName(),
                            object.getClass().getCanonicalName())
            );
        }
        Iterator<PropertyConverter> it = resolved.iterator();
        T converted = null;
        while (it.hasNext() && converted == null) {
            PropertyConverter converter = it.next();
            converted = converter.convert(object, classType);
        }
        return converted;
    }

    private static List<PropertyConverter> resolves(final List<PropertyConverter> converters,
                                                    final Object object,
                                                    final Class<?> classType) {

        List<PropertyConverter> specificConverters = findSpecificConverterToConvert(converters, object, classType);
        if (!specificConverters.isEmpty()) {
            return specificConverters;
        }

        List<PropertyConverter> genericConverters = findGenericConverterToRead(converters, object, classType);
        if (!genericConverters.isEmpty()) {
            return genericConverters;
        }

        return Collections.emptyList();
    }

    private static List<PropertyConverter> findGenericConverterToRead(final List<PropertyConverter> converters,
                                                                      final Object object,
                                                                      final Class<?> classType) {
        return converters
                .stream()
                .filter(converter -> !isSpecificConverter(converter))
                .filter(converter -> converter.canConvert(object, classType))
                .collect(Collectors.toList());
    }

    private static List<PropertyConverter> findSpecificConverterToConvert(final List<PropertyConverter> converters,
                                                                          final Object object,
                                                                          final Class<?> classType) {
        return converters
                .stream()
                .filter(converter -> isConverterSpecificForType(classType, converter))
                .filter(converter -> converter.canConvert(object, classType))
                .collect(Collectors.toList());
    }

    private static boolean isConverterSpecificForType(final Class<?> type,
                                                      final PropertyConverter converter) {
        if (isSpecificConverter(converter)) {
            Class<?>[] specificTargetClasses = converter.getSpecificTargetClasses();
            List<Class<?>> l = Arrays
                    .stream(specificTargetClasses)
                    .filter(clazz -> clazz.isAssignableFrom(type))
                    .collect(Collectors.toList());
            return l.size() > 0;
        }
        return false;
    }

    private static boolean isSpecificConverter(final PropertyConverter converter) {
        return converter.getSpecificTargetClasses() != null && converter.getSpecificTargetClasses().length > 0;
    }

}
