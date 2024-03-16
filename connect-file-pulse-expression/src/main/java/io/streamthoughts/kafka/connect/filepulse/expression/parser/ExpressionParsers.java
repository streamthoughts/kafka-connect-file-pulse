/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.parser;

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.antlr4.Antlr4ExpressionParser;

public class ExpressionParsers  {

    private static final Antlr4ExpressionParser DEFAULT = new Antlr4ExpressionParser();

    /**
     * Static helper method to parse an expression using default {@link ExpressionParser}.
     *
     * @param expression      the expression to parse.
     *
     * @return                a new {@link Expression}.
     */
    public static Expression parseExpression(final String expression) {
        return DEFAULT.parseExpression(expression);
    }

    /**
     * Static helper method to parse an expression using default {@link ExpressionParser}.
     *
     * @param expression      the expression to parse.
     * @param defaultScope    the default scope.
     *
     * @return                a new {@link Expression}.
     */
    public static Expression parseExpression(final String expression, final String defaultScope) {
        return DEFAULT.parseExpression(expression, defaultScope);
    }
}
