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
package io.streamthoughts.kafka.connect.filepulse.expression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutor;

import java.util.List;

public class FunctionExpression implements Expression {

    private final String original;
    private final Expression valueExpression;
    private final ExpressionFunctionExecutor function;

    /**
     * Creates a new {@link FunctionExpression} instance.
     *
     * @param original         the original string expression.
     * @param valueExpression  the expression value
     * @param function         the function to be apply on the acceded value.
     */
    public FunctionExpression(final String original,
                              final Expression valueExpression,
                              final ExpressionFunctionExecutor function) {
        this.original = original;
        this.valueExpression = valueExpression;
        this.function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue readValue(final EvaluationContext context) {
        return readValue(context, TypedValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T readValue(final EvaluationContext context, final Class<T> expectedType) {
        TypedValue returned = valueExpression.readValue(context, TypedValue.class);
        final Object evaluated = function.execute(returned);

        if (evaluated != null && expectedType.isAssignableFrom(evaluated.getClass())) {
            return (T)evaluated;
        }

        final List<PropertyConverter> converters = context.getPropertyConverter();
        return Converters.converts(converters, evaluated, expectedType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(final Object value, final EvaluationContext context) {
        throw new UnsupportedOperationException("functional expression cannot be used to write value");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String originalExpression() {
        return original;
    }

    @Override
    public String toString() {
        return "[" +
                "original=" + original +
                ", valueExpression=" + valueExpression +
                ", function=" + function +
                ']';
    }
}
