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
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.GenericArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConcatWs implements ExpressionFunction {

    private static final String SEPARATOR_ARG = "separator";
    private static final String PREFIX_ARG = "prefix";
    private static final String SUFFIX_ARG = "suffix";

    /**
     * {@inheritDoc}
     */
    @Override
    public Arguments<?> prepare(final Expression[] args) {
        if (args.length < 3) {
            return Arguments.of(
                new MissingArgumentValue(SEPARATOR_ARG),
                new MissingArgumentValue(PREFIX_ARG),
                new MissingArgumentValue(SUFFIX_ARG)
            );
        }

        List<ExpressionArgument> arguments = new ArrayList<>(args.length);
        arguments.add(new ExpressionArgument(SEPARATOR_ARG, args[0]));
        arguments.add(new ExpressionArgument(PREFIX_ARG, args[1]));
        arguments.add(new ExpressionArgument(SUFFIX_ARG, args[2]));

        for (int i = 3; i < args.length; i++) {
            arguments.add(new ExpressionArgument("expr" + (i-2), args[i]));
        }

        return new Arguments<>(arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue apply(Arguments<GenericArgument> args) {

        final String delimiter = ((TypedValue)args.get(0).value()).getString();
        final String prefix = ((TypedValue)args.get(1).value()).getString();
        final String suffix = ((TypedValue)args.get(2).value()).getString();

        List<GenericArgument> values = args.get(3, args.size());
        String concat = values.stream()
            .map(it -> (TypedValue) it.value())
            .filter(TypedValue::isNotNull)
            .map(TypedValue::getString)
            .collect(Collectors.joining(delimiter, prefix, suffix));
        return TypedValue.string(concat);
    }
}
