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
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.GenericArgument;

import java.util.regex.Pattern;

/**
 * Replaces every subsequence of the input sequence that matches the
 * pattern with the given replacement string.
 */
public class ReplaceAll implements ExpressionFunction {

    private static final String FIELD_ARG = "field";
    private static final String PATTERN_ARG = "pattern";
    private static final String REPLACEMENT_ARG = "replacement";

    /**
     * {@inheritDoc}
     */
    @Override
    public Arguments<?> prepare(final Expression[] args) {
        if (args.length < 3) {
            return Arguments.of(
                 new MissingArgumentValue(FIELD_ARG),
                 new MissingArgumentValue(PATTERN_ARG),
                 new MissingArgumentValue(REPLACEMENT_ARG)
            );
        }

        return Arguments.of(
            new ExpressionArgument(FIELD_ARG, args[0]),
            new GenericArgument<>(PATTERN_ARG, Pattern.compile(((ValueExpression)args[1]).value().getString())),
            new ExpressionArgument(REPLACEMENT_ARG,  args[2])
        );
    }

    /**
     * {@inheritDoc}
     */
    public TypedValue apply(final Arguments<GenericArgument> args) {
        Pattern pattern = args.valueOf(PATTERN_ARG);
        TypedValue replacement = args.valueOf(REPLACEMENT_ARG);
        TypedValue value = args.valueOf(FIELD_ARG);
        String result = pattern.matcher(value.getString()).replaceAll(replacement.getString());
        return TypedValue.string(result);
    }
}

