/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.objects;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

public class Exists implements ExpressionFunction {

    private static final String OBJECT_ARG = "object_expr";
    private static final String FIELD_ARG = "field_expr";

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private String syntax() {
                return String.format("syntax %s(<%s>, <%s>)", name(), OBJECT_ARG, FIELD_ARG);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                  if (args.length < 2) {
                    throw new ExpressionException("Missing required arguments: " + syntax());
                }

                return Arguments.of(OBJECT_ARG, args[0], FIELD_ARG, args[1]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                final TypedValue object = context.get(OBJECT_ARG);
                final TypedValue path = context.get(FIELD_ARG);

                if (object.type() != Type.STRUCT && object.type() != Type.NULL) {
                    throw new ExpressionException("Expected type [STRUCT|NULL], was " + object.type());
                }

                if (object.type() == Type.NULL) return TypedValue.bool(false);

                final TypedStruct struct = object.getStruct();
                final boolean exists = struct.exists(path.getString());
                return TypedValue.bool(exists);
            }
        };
    }
}
