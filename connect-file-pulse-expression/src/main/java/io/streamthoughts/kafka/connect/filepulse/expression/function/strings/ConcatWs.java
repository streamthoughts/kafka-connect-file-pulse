/*
 * Copyright 2019-2021 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Argument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
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
    public Instance get() {
        return new AbstractExpressionFunctionInstance() {

            private String syntax() {
                return String.format(
                    "syntax %s(<%s>, <%s>, <%s> [, <expressions])",
                    name(),
                    SEPARATOR_ARG,
                    PREFIX_ARG,
                    SUFFIX_ARG
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

                List<Argument> arguments = new ArrayList<>(args.length);
                arguments.add(new ExpressionArgument(SEPARATOR_ARG, args[0]));
                arguments.add(new ExpressionArgument(PREFIX_ARG, args[1]));
                arguments.add(new ExpressionArgument(SUFFIX_ARG, args[2]));

                for (int i = 3; i < args.length; i++) {
                    arguments.add(new ExpressionArgument("expr" + (i-2), args[i]));
                }

                return new Arguments(arguments);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
                return apply(
                        context.get(0).getString(),
                        context.get(1).getString(),
                        context.get(2).getString(),
                        context.get(3, context.size())
                );
            }

            public TypedValue apply(final String delimiter,
                                    final String prefix,
                                    final String suffix,
                                    final List<TypedValue> values) {
                String concat = values.stream()
                        .filter(TypedValue::isNotNull)
                        .map(TypedValue::getString)
                        .collect(Collectors.joining(delimiter, prefix, suffix));
                return TypedValue.string(concat);
            }
        };
    }
}
