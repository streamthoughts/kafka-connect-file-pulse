/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.converter;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;

public class PrimitiveConverter implements PropertyConverter {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canConvert(final Object o, final Class<?> classType) {
        return Type.forClass(classType) != null;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T convert(final Object o, final Class<T> classType) {
        Type type = Type.forClass(classType);
        if (type == null) {
            throw new ConversionException(
                    String.format(
                            "Cannot convert object of type '%s' into expected type '%s'",
                            o.getClass().getCanonicalName(),
                            classType.getCanonicalName()
                    )
            );
        }

        if (o instanceof TypedValue) {
            final TypedValue typed = (TypedValue) o;
            if (typed.type() == type) {
                return typed.value();
            }
            return (T) type.convert(typed.value());
        }
        return (T) type.convert(o);
    }
}
