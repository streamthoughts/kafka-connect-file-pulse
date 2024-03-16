/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.Converters;
import io.streamthoughts.kafka.connect.filepulse.expression.converter.PropertyConverter;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutor;
import java.util.List;

public class FunctionExpression extends AbstractExpression {

    private final ExpressionFunctionExecutor functionExecutor;

    /**
     * Creates a new {@link FunctionExpression} instance.
     *
     * @param originalExpression the original string expression.
     * @param functionExecutor   the function to be apply on the acceded value.
     */
    public FunctionExpression(final String originalExpression,
                              final ExpressionFunctionExecutor functionExecutor) {
        super(originalExpression);
        this.functionExecutor = functionExecutor;
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
        final Object evaluated = functionExecutor.execute(context);

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
    public boolean canWrite() {
        return false;
    }

    public ExpressionFunctionExecutor getFunctionExecutor() {
        return functionExecutor;
    }

    @Override
    public String toString() {
        return "[" +
                "originalExpression=" + originalExpression() +
                ", function=" + functionExecutor +
                ']';
    }
}
