/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.parser;

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;

/**
 * Default interface to build a new @{link Expression} instance from a given string.
 */
public interface ExpressionParser {

    /**
     * Build a new {@link Expression} instance for the specified expression string.
     * @param expression    the expression to parse.
     * @return  a new {@link Expression} instance
     *
     * @throws ExpressionException if an invalid {@literal expression} is given.
     */
    Expression parseExpression(final String expression) throws ExpressionException;

}
