/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import org.junit.Test;

public class SubstitutionExpressionTest {

    private static final String DEFAULT_EXPRESSION_STRING = "{{ value }}";
    private static final String SUFFIX = "-suffix";
    private static final String PREFIX = "prefix-";

    private static final EvaluationContext context = new StandardEvaluationContext(new Object());

    private static final String EVALUATED_VALUE = "value";

    private static final Expression DEFAULT_EXPRESSION = ValueExpression.of(EVALUATED_VALUE);

    @Test
    public void testGivenNullExpressionReplacement() {
        final String str = "{{ null }}";
        SubstitutionExpression expression = new SubstitutionExpression(
                str,
                0,
                str.length(),
                new ValueExpression(str, null));

        assertNull(expression.readValue(context));
    }

    @Test
    public void testGivenSingleExpressionReplacement() {
        SubstitutionExpression expression = new SubstitutionExpression(
            DEFAULT_EXPRESSION_STRING,
            0,
            DEFAULT_EXPRESSION_STRING.length(),
            DEFAULT_EXPRESSION);

        TypedValue evaluated = expression.readValue(context, TypedValue.class);

        assertNotNull(evaluated);
        assertEquals(Type.STRING, evaluated.type());
        assertEquals(EVALUATED_VALUE, evaluated.getString());
    }

    @Test
    public void testGivenPrefixedExpressionWithReplacement() {
        final int startIndex = PREFIX.length();
        SubstitutionExpression expression = new SubstitutionExpression(PREFIX + DEFAULT_EXPRESSION_STRING,
                startIndex,
                startIndex + DEFAULT_EXPRESSION_STRING.length(),
                DEFAULT_EXPRESSION);

        TypedValue evaluated = expression.readValue(context, TypedValue.class);

        assertNotNull(evaluated);
        assertEquals(Type.STRING, evaluated.type());
        assertEquals(PREFIX + EVALUATED_VALUE, evaluated.getString());
    }

    @Test
    public void testGivenSuffixedExpressionWithReplacement() {
        final int startIndex = 0;
        final int endIndex = DEFAULT_EXPRESSION_STRING.length();
        SubstitutionExpression expression = new SubstitutionExpression(DEFAULT_EXPRESSION_STRING + SUFFIX, startIndex, endIndex, DEFAULT_EXPRESSION);

        TypedValue evaluated = expression.readValue(context, TypedValue.class);

        assertNotNull(evaluated);
        assertEquals(Type.STRING, evaluated.type());
        assertEquals(EVALUATED_VALUE + SUFFIX, evaluated.getString());
    }

    @Test
    public void testGivenBetweenExpressionWithReplacement() {
        final String original = PREFIX + DEFAULT_EXPRESSION_STRING + SUFFIX;
        SubstitutionExpression expression = new SubstitutionExpression(original, PREFIX.length(), PREFIX.length() + DEFAULT_EXPRESSION_STRING.length(), DEFAULT_EXPRESSION);

        TypedValue evaluated = expression.readValue(context, TypedValue.class);

        assertNotNull(evaluated);
        assertEquals(Type.STRING, evaluated.type());
        assertEquals(PREFIX + EVALUATED_VALUE + SUFFIX, evaluated.getString());
    }
}