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
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.GenericArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;

import java.util.Arrays;
import java.util.regex.Pattern;

public class Split implements ExpressionFunction {

    private static final String FIELD_ARG = "field";
    private static final String REGEX_ARG = "separator";
    private static final String LIMIT_ARG = "limit";

    /**
     * {@inheritDoc}
     */
    @Override
    public Arguments<?> prepare(final Expression[] args) {
        if (args.length < 2) {
            return Arguments.of(
                new MissingArgumentValue(FIELD_ARG),
                new MissingArgumentValue(REGEX_ARG)
            );
        }

        int limitArgument = 0;
        if (args.length ==3 ) {
            limitArgument =  ((ValueExpression) args[2]).value().getInt();
        }

        final String regex = ((ValueExpression) args[1]).value().getString();
        Object regexArgument;
        if (StringUtils.isFastSplit(regex)) {
            regexArgument = regex;
        } else {
            regexArgument = Pattern.compile(regex);
        }

        return Arguments.of(
            new ExpressionArgument(FIELD_ARG, args[0]),
            new GenericArgument<>(REGEX_ARG, regexArgument),
            new GenericArgument<>(LIMIT_ARG, limitArgument)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue apply(Arguments<GenericArgument> args) {
        TypedValue field = args.valueOf(FIELD_ARG);
        Object regex = args.valueOf(REGEX_ARG);
        Integer limit = args.valueOf(LIMIT_ARG);

        final String[] split;
        if (regex instanceof Pattern) {
            split = ((Pattern)regex).split(field.getString(), limit);
        } else  {
            split = field.getString().split(regex.toString(), limit);
        }
        return TypedValue.array(Arrays.asList(split), Type.STRING);
    }
}
