/*
 * Copyright 2019 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.expression.FunctionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.PropertyExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.regex.RegexExpressionParser;
import org.junit.Assert;
import org.junit.Test;

public class RegexExpressionParserTest {

    private static final String DEFAULT_ROOT_OBJECT = "defaultRoot";

    @Test
    public void testGivenExpressionWithStaticValue() {
        String originalExpression = "my-simple-value";
        ValueExpression expression = assertExpressionAndGet(
            originalExpression,
            ValueExpression.class);

        Assert.assertEquals(originalExpression, expression.originalExpression());
        Assert.assertEquals(originalExpression, expression.readValue(null, String.class));
    }

    @Test
    public void testGivenSubstitutionExpressionWithNotRootObject() {
        final String originalExpression = "myField";
        PropertyExpression expression = assertExpressionAndGet(
            originalExpression,
            PropertyExpression.class);

        Assert.assertEquals(PropertyExpression.class, expression.getClass());
        assertSimpleExpression(expression, originalExpression, DEFAULT_ROOT_OBJECT, "myField");
    }

    @Test
    public void testGivenExpressionWithRootObjectAndNAttributeAccess() {
        final String originalExpression = "$dummyRootObject";
        PropertyExpression expression = assertExpressionAndGet(
                originalExpression,
                PropertyExpression.class);

        assertSimpleExpression(expression, originalExpression, "dummyRootObject", "null");
    }

    @Test
    public void testGivenExpressionWithRootObjectAndAttributeAccess() {
        final String originalExpression = "$dummyRootObject.dummyAttribute";
        PropertyExpression expression = assertExpressionAndGet(
                originalExpression,
                PropertyExpression.class);

        assertSimpleExpression(expression, originalExpression, "dummyRootObject", "dummyAttribute");
    }

    @Test
    public void testGivenFunctionalExpressionWithAttributeAccessorAndNoArgs() {
        final String originalExpression = "lowercase(attribute)";
        FunctionExpression expression = assertExpressionAndGet(
                originalExpression,
                FunctionExpression.class);

        assertFunctionExpression(
            expression,
            originalExpression,
            "[original=attribute, rootObject=defaultRoot, attribute=attribute]",
            "[name='lowercase', arguments=[]]"
        );
    }

    @Test
    public void testGivenFunctionalExpressionWithRootAndAttributeAccessorAndNoArgs() {
        final String originalExpression = "lowercase($root.attribute)";
        FunctionExpression expression = assertExpressionAndGet(
                originalExpression,
                FunctionExpression.class);

        assertFunctionExpression(
                expression,
                originalExpression,
                "[original=$root.attribute, rootObject=root, attribute=attribute]",
                "[name='lowercase', arguments=[]]"
        );
    }


    @Test
    public void testGivenFunctionExpressionWithRootAccessorAndSingleArg() {
        final String originalExpression = "extract_array($root, 0)";

        FunctionExpression expression = assertExpressionAndGet(
                originalExpression,
                FunctionExpression.class);

        assertFunctionExpression(
                expression,
                originalExpression,
                "[original=$root, rootObject=root, attribute=null]",
                "[name='extract_array', arguments=[{name='index', value=0, errorMessages=[]}]]"
        );
    }

    @Test
    public void testGivenFunctionExpressionWithRootAndAttributeAccessorAndSingleArg() {
        final String originalExpression = "extract_array($root.attribute, 0)";

        FunctionExpression expression = assertExpressionAndGet(
                originalExpression,
                FunctionExpression.class);

        assertFunctionExpression(
                expression,
                originalExpression,
                "[original=$root.attribute, rootObject=root, attribute=attribute]",
                "[name='extract_array', arguments=[{name='index', value=0, errorMessages=[]}]]"
        );
    }

    @SuppressWarnings("unchecked")
    private <T extends Expression> T assertExpressionAndGet(final String originalExpression,
                                                            final Class<T> expected) {
        Expression e = new RegexExpressionParser()
                .parseExpression(originalExpression, DEFAULT_ROOT_OBJECT, false);

        Assert.assertEquals(expected, e.getClass());

        return (T)e;
    }

    private void assertSimpleExpression(final Expression expression,
                                        final String originalExpression,
                                        final String rootObject,
                                        final String attribute) {
        Assert.assertEquals(
                "[original=" + originalExpression +
                        ", rootObject=" + rootObject +
                        ", attribute=" + attribute + "]",
                expression.toString());
    }

    private void assertFunctionExpression(final Expression expression,
                                        final String originalExpression,
                                        final String valueExpression,
                                        final String function) {
        Assert.assertEquals(
                "[original=" + originalExpression +
                        ", valueExpression=" + valueExpression +
                        ", function=" + function + "]",
                expression.toString());
    }
}