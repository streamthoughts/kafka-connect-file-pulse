/*
 * Copyright 2021 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunctionExecutors;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class AndTest {

    private static final StandardEvaluationContext EMPTY_CONTEXT  = new StandardEvaluationContext(new Object());

    @Test
    public void test_expression_and() {
        Expression expressionTrue = ExpressionParsers.parseExpression("{{ and(true, true) }}");
        Expression expressionFalse = ExpressionParsers.parseExpression("{{ and(true, false) }}");
        Assert.assertTrue(expressionTrue.readValue(EMPTY_CONTEXT, TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void test_expressions_should_not_be_all_evaluated() {

        // Given
        final TestFunction function1 = new TestFunction("test1", TypedValue.bool(true));
        ExpressionFunctionExecutors.INSTANCE.register(function1);

        // When
        final Expression expressionTrue = ExpressionParsers.parseExpression("{{ and(false, test1()) }}");

        // Then
        Assert.assertFalse(expressionTrue.readValue(EMPTY_CONTEXT, TypedValue.class).value());
        Assert.assertFalse(function1.instances.get(0).called.get());

        // Given
        final TestFunction function2 = new TestFunction("test2", TypedValue.bool(true));
        ExpressionFunctionExecutors.INSTANCE.register(function2);

        // When
        final Expression expressionFalse = ExpressionParsers.parseExpression("{{ and(true, test2()) }}");

        // Then
        Assert.assertTrue(expressionFalse.readValue(EMPTY_CONTEXT, TypedValue.class).value());
        Assert.assertTrue(function2.instances.get(0).called.get());
    }

    static class TestFunction implements ExpressionFunction {

        private final String name;

        private final TypedValue value;

        public TestFunction(final String name, final TypedValue value) {
            this.name = name;
            this.value = value;
        }

        List<TestInstance> instances = new LinkedList<>();

        @Override
        public String name() {
            return name;
        }

        @Override
        public Instance get() {
            TestInstance i = new TestInstance(value);
            instances.add(i);
            return i;
        }

        static class TestInstance implements Instance {

            final TypedValue value;

            final AtomicBoolean called = new AtomicBoolean(false);

            public TestInstance(final TypedValue value) {
                this.value = value;
            }

            @Override
            public TypedValue invoke(final EvaluationContext context,
                                     final Arguments arguments) throws ExpressionException {
                called.set(true);
                return value;
            }
        }
    }
}
