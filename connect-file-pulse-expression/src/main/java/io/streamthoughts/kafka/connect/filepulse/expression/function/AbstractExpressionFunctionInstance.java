/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import java.util.stream.IntStream;

public abstract class AbstractExpressionFunctionInstance implements ExpressionFunction.Instance {

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue invoke(final EvaluationContext context,
                             final Arguments arguments) throws ExpressionException {

        final EvaluatedExecutionContext executionContext = new EvaluatedExecutionContext();

        IntStream.range(0, arguments.size()).forEachOrdered(i -> {
            final Argument argument = arguments.get(i);
            final TypedValue value = argument.evaluate(context);
            executionContext.addArgument(argument.name(), i, value);
        });

        return invoke(executionContext);
    }

    /**
     * Executes the function with the specific context.
     *
     * @param context the {@link EvaluatedExecutionContext}.
     * @return the function result.
     * @throws ExpressionException if the function execution failed.
     */
    public abstract TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException;
}
