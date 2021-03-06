/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function.impl;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.GenericArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;

public class Equals implements ExpressionFunction {

    private static final String OBJECT_L_ARG = "left";
    private static final String OBJECT_R_ARG = "right";

    /**
     * {@inheritDoc}
     */
    @Override
    public Arguments prepare(final Expression[] args) {
        if (args.length < 2) {
            return Arguments.of(
                new MissingArgumentValue(OBJECT_L_ARG),
                new MissingArgumentValue(OBJECT_R_ARG)
            );
        }
        return Arguments.of(
            new ExpressionArgument(OBJECT_L_ARG, args[0]),
            new ExpressionArgument(OBJECT_R_ARG, args[1])
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue apply(final Arguments<GenericArgument> args) {
        final TypedValue left = args.valueOf(OBJECT_L_ARG);
        final TypedValue right = args.valueOf(OBJECT_R_ARG);

        // attempt to convert argument value to the field value before applying equality.
        final Object leftValue = left.value();
        final Object rightValue = right.as(left.type()).value();
        return TypedValue.of(leftValue.equals(rightValue), Type.BOOLEAN);
    }
}
