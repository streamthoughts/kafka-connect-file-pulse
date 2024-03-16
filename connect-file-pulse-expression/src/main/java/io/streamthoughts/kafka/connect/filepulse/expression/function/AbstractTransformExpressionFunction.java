/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;

public abstract class AbstractTransformExpressionFunction implements ExpressionFunction {

    private static final String FIELD_ARG = "field_expr";

    public abstract TypedValue transform(final TypedValue value);

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                if (args.length < 1) {
                    throw new ExpressionException(String.format(
                            "Missing required arguments: %s(<%s>)", name(), FIELD_ARG)
                    );
                }

                return Arguments.of(FIELD_ARG, args[0]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                return transform(context.get(0));
            }
        };
    }
}
