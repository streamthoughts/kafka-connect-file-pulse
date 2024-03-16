/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

public class StartsWith implements ExpressionFunction {

    private static final String FIELD_ARG = "field_expr";
    private static final String PREFIX_ARG = "prefix";

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private String syntax() {
                return String.format("syntax %s(<%s>, <%s>)", name(), FIELD_ARG, PREFIX_ARG);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                if (args.length > 2) {
                    throw new ExpressionException("Too many arguments: " + syntax());
                }
                if (args.length < 2) {
                    throw new ExpressionException("Missing required arguments: " + syntax());
                }

                return Arguments.of(FIELD_ARG, args[0], PREFIX_ARG, args[1]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                TypedValue field = context.get(0);
                TypedValue prefix = context.get(1);
                return TypedValue.bool(field.getString().startsWith(prefix.getString()));
            }
        };
    }
}
