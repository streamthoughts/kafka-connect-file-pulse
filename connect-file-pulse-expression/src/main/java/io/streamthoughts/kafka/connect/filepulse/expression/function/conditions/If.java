/*
 * Copyright 2021 StreamThoughts.
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
