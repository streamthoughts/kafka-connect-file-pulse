/*
 * Copyright 2019-2021 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.conditions;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

public class Equals implements ExpressionFunction {

    private static final String OBJECT_L_ARG = "object_expr1";
    private static final String OBJECT_R_ARG = "object_expr2";

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private String syntax() {
                return String.format("syntax %s(<%s>, <%s>)", name(), OBJECT_L_ARG, OBJECT_R_ARG);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                if (args.length < 2) {
                    throw new ExpressionException("Missing required arguments: " + syntax());
                }
                return Arguments.of(OBJECT_L_ARG, args[0], OBJECT_R_ARG, args[1]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                final TypedValue left = context.get(OBJECT_L_ARG);
                final TypedValue right = context.get(OBJECT_R_ARG);

                if (left.isNull() && right.isNull())
                    return TypedValue.bool(true);

                if (left.isNull() || right.isNull())
                    return TypedValue.bool(false);

                // attempt to convert argument value to the field value before applying equality.
                final Object leftValue = left.value();
                final Object rightValue = right.as(left.type()).value();
                return TypedValue.of(leftValue.equals(rightValue), Type.BOOLEAN);
            }
        };
    }
}
