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

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.SubstitutionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple {@link ExpressionParser} implementation that use regex to parse expression.
 */
public class RegexExpressionParser implements ExpressionParser {

    private static Pattern SUBSTITUTION_EXPRESSION_PATTERN =
            Pattern.compile("(?<expression>\\{\\{\\s*.+?\\s*\\}\\})+?");

    private final RegexExpressionMatchers matchers;

    /**
     * Creates a new {@link RegexExpressionParser} instance.
     */
    public RegexExpressionParser() {
        matchers = new RegexExpressionMatchers();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Expression parseExpression(final String expression) {
        return parseExpression(expression, null);
    }

    public Expression parseExpression(final String expression, final String defaultRootObject) {
        return parseExpression(expression, defaultRootObject,true);
    }

    public Expression parseExpression(final String expression,
                                      final boolean substitution) {
        return parseExpression(expression, null, substitution);
    }

    public Expression parseExpression(final String expression,
                                      final String defaultRootObject,
                                      final boolean substitution) {

        Matcher matcher = SUBSTITUTION_EXPRESSION_PATTERN.matcher(expression);

        Expression compiledExpression = null;

        if (substitution) {
            while (matcher.find()) {

                final String substitutionExpression = matcher.group();
                final Expression replacement = matchers.matches(
                    substitutionExpression,
                    defaultRootObject,
                    substitution);

                if (replacement == null) {
                    throw new ExpressionException("Invalid substitution expression : '"
                            + substitutionExpression
                            + "' (original expression = '" + expression + "')");
                }

                if (compiledExpression == null) {
                    compiledExpression = new SubstitutionExpression(
                        expression,
                        matcher.start(),
                        matcher.end(),
                        replacement
                    );
                } else {
                    ((SubstitutionExpression)compiledExpression)
                        .addReplacement(
                            matcher.start(),
                            matcher.end(),
                            replacement
                        );
                }
            }
        } else {
            compiledExpression = matchers.matches(expression, defaultRootObject, false);
        }

        if (compiledExpression == null) {
            return new ValueExpression(expression);
        }

        return compiledExpression;
    }
}
