/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
