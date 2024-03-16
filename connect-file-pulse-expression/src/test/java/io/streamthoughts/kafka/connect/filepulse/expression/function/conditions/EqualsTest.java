/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.conditions;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import org.junit.Assert;
import org.junit.Test;

public class EqualsTest {

    private static final StandardEvaluationContext EMPTY_CONTEXT  = new StandardEvaluationContext(new Object());

    @Test
    public void test_equals_given_two_equals_strings() {
        // Given
        Expression expression = parseExpression("{{ equals('foo', 'foo') }}");

        // When
        final TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);

        // Then
        Assert.assertTrue(result.value());
    }

    @Test
    public void test_equals_return_false_given_a_null_value() {
        // Given
        Expression expression = parseExpression("{{ equals($value, 'foo') }}");

        final StandardEvaluationContext context = new StandardEvaluationContext(TypedValue.string(null));

        // When
        final TypedValue result = expression.readValue(context, TypedValue.class);

        // Then
        Assert.assertFalse(result.value());
    }

    @Test
    public void test_equals_return_true_given_null_values() {
        // Given
        Expression expression = parseExpression("{{ equals($value, null) }}");

        final StandardEvaluationContext context = new StandardEvaluationContext(TypedValue.string(null));

        // When
        final TypedValue result = expression.readValue(context, TypedValue.class);

        // Then
        Assert.assertTrue(result.value());
    }
}