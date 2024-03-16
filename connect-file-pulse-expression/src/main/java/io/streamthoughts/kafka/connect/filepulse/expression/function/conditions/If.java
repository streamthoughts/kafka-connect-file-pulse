/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.conditions;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

public class If implements ExpressionFunction {

    private static final String BOOLEAN_EXPRESSION_ARG = "booleanExpression";
    private static final String VALUE_IF_TRUE_ARG = "valueIfTrue";
    private static final String VALUE_IF_FALSE_ARG = "valueIfFalse";

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private String syntax() {
                return String.format(
                        "syntax %s(<%s>, <%s>, <%s>)",
                        name(),
                        BOOLEAN_EXPRESSION_ARG,
                        VALUE_IF_TRUE_ARG,
                        VALUE_IF_FALSE_ARG
                );
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                if (args.length < 3) {
                    throw new ExpressionException("Missing required arguments: " + syntax());
                }

                return Arguments.of(
                        BOOLEAN_EXPRESSION_ARG, args[0],
                        VALUE_IF_TRUE_ARG, args[1],
                        VALUE_IF_FALSE_ARG, args[2]
                );
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext ctx) throws ExpressionException {
                final TypedValue condition = ctx.get(BOOLEAN_EXPRESSION_ARG);
                return condition.getBool() ? ctx.get(VALUE_IF_TRUE_ARG) : ctx.get(VALUE_IF_FALSE_ARG);
            }
        };
    }
}
