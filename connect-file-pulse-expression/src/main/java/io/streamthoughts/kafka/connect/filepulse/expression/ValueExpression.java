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
package io.streamthoughts.kafka.connect.filepulse.expression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;
import java.util.List;
import java.util.Objects;

public class ValueExpression extends AbstractExpression {

    private final Object value;

    /**
     * Static helper to create a new {@link ValueExpression} for the given expression and value.
     *
     * @param originalExpression    the original string expression.
     * @return                      a new {@link ValueExpression}.
     */
    public static ValueExpression of(final String originalExpression) {
        return new ValueExpression(originalExpression, originalExpression);
    }

    /**
     * Creates a new {@link ValueExpression} instance.
     *
     * @param originalExpression the original string expression.
     * @param value              the static value.
     */
    public ValueExpression(final String originalExpression,
                           final Object value) {
        super(originalExpression);
        this.value = value;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue readValue(final EvaluationContext context) {
        return value();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
        if (value == null)
            return null;

        if (expectedType == String.class)
            return (T) value().getString();

        final List<PropertyConverter> converters = context.getPropertyConverter();
        return Converters.converts(converters, value, expectedType);
    }

    public TypedValue value() {
        return TypedValue.any(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(final Object value, final EvaluationContext context) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canWrite() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ValueExpression)) return false;
        if (!super.equals(o)) return false;
        ValueExpression that = (ValueExpression) o;
        return Objects.equals(value, that.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return value().getString();
    }
}
