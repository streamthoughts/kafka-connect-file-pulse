/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.objects;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

/**
 * Simple function to convert a field into a new type.
 */
public class Converts implements ExpressionFunction {

    private static final String FIELD_ARG = "field_expr";
    private static final String TYPE_ARG = "type";

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private String syntax() {
                return String.format("syntax %s(<%s>, <%s>)", name(), FIELD_ARG, TYPE_ARG);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                if (args.length < 2) {
                    throw new ExpressionException("Missing required arguments: " + syntax());
                }
                return Arguments.of(FIELD_ARG, args[0], TYPE_ARG, args[1]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                final TypedValue value = context.get(FIELD_ARG);
                final TypedValue type  = context.get(TYPE_ARG);
                return value.as(Type.valueOf(type.value()));
            }
        };
    }
}