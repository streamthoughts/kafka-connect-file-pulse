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
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.GenericArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;

public class If implements ExpressionFunction {

    private static final String BOOLEAN_EXPRESSION_ARG = "booleanExpression";
    private static final String VALUE_IF_TRUE_ARG = "valueIfTrue";
    private static final String VALUE_IF_FALSE_ARG = "valueIfFalse";

    /**
     * {@inheritDoc}
     */
    @Override
    public Arguments<?> prepare(final Expression[] args) {
        if (args.length < 3) {
            return Arguments.of(
                new MissingArgumentValue(BOOLEAN_EXPRESSION_ARG),
                new MissingArgumentValue(VALUE_IF_TRUE_ARG),
                new MissingArgumentValue(VALUE_IF_FALSE_ARG)
            );
        }

        return Arguments.of(
            new ExpressionArgument(BOOLEAN_EXPRESSION_ARG, args[0]),
            new ExpressionArgument(VALUE_IF_TRUE_ARG, args[1]),
            new ExpressionArgument(VALUE_IF_FALSE_ARG, args[2])
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue apply(Arguments<GenericArgument> args) {
        final TypedValue condition = args.valueOf(BOOLEAN_EXPRESSION_ARG);
        return condition.getBool() ? args.valueOf(VALUE_IF_TRUE_ARG) : args.valueOf(VALUE_IF_FALSE_ARG);
    }
}
