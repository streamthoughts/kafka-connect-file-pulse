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
