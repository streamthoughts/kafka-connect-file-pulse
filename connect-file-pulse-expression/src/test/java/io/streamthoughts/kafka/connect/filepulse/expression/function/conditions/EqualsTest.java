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