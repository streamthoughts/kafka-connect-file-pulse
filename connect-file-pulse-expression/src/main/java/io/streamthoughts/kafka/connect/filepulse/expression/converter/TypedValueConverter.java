/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.converter;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;

/**
 * Simple converter to wrap an object into a {@link TypedValue}.
 */
public class TypedValueConverter implements PropertyConverter {

    private final static Class<?>[] specific = new Class<?>[]{TypedValue.class};

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?>[] getSpecificTargetClasses() {
        return specific;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canConvert(final Object o, final Class<?> classType) {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T convert(final Object o, final Class<T> classType) throws ConversionException {
        return (T) TypedValue.any(o);
    }
}
