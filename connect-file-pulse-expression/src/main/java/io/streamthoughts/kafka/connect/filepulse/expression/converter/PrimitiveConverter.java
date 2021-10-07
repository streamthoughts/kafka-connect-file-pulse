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
