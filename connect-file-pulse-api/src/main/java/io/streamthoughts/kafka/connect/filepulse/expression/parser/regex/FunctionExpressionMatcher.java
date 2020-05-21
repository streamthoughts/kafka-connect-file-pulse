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
package io.streamthoughts.kafka.connect.filepulse.expression.parser.regex;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.FunctionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutor;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutorFactory;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FunctionExpressionMatcher implements RegexExpressionMatcher {

    private static final String GROUP_METHOD      = "method";
    private static final String GROUP_VALUE       = "value";
    private static final String GROUP_ARGS        = "args";

    private static Pattern PATTERN =
            Pattern.compile("^(?:(?<method>[\\w_]+)\\((?:(?<value>.+?))(?:,(?<args>.+))*?\\))$");


    private final RegexExpressionMatchers matchers;

    /**
     * Creates a new {@link RegexExpressionParser} instance.
     */
    FunctionExpressionMatcher() {
        matchers = new RegexExpressionMatchers();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public FunctionExpression matches(final String expression,
                                      final String defaultRootObject,
                                      final boolean substitution) {

        final String trimmed = substitution ? trimSubstitutionExpression(expression) : expression;
        Matcher matcher = PATTERN.matcher(trimmed);
        if (matcher.matches()) {
            final String value  = matcher.group(GROUP_VALUE);
            final String method = matcher.group(GROUP_METHOD);
            final String args   = matcher.group(GROUP_ARGS);


            final Expression valueExpression = matchers.matches(value, defaultRootObject, false);

            if (valueExpression == null) {
                throw new ExpressionException("Invalid function expression : '" + expression + "'");
            }

            TypedValue[] parsedArgs = new TypedValue[]{};
            if (args != null) {
                parsedArgs = Arrays
                        .stream(args.split(","))
                        .map(s -> TypedValue.string(s.trim()))
                        .toArray(TypedValue[]::new);
            }

            ExpressionFunctionExecutor function = ExpressionFunctionExecutorFactory
                    .getInstance()
                    .make(method, parsedArgs);

            return new FunctionExpression(
                    expression,
                    valueExpression,
                    function);
        }
        return null;
    }

    private static String trimSubstitutionExpression(final String expression) {
        return expression.substring(2, expression.length() - 2).trim();
    }
}
