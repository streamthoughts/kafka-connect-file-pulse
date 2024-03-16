/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class SplitTest {

    private static final StandardEvaluationContext EMPTY_CONTEXT  = new StandardEvaluationContext(new Object());

    @Test
    public void should_execute_function_split_given_single_character() {
        Expression expression =  parseExpression("{{ split('one,two,three', ',') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        Assert.assertEquals(Type.ARRAY, result.type());
        Assert.assertEquals(3, result.getArray().size());
        Assert.assertEquals("one", new ArrayList<>(result.getArray()).get(0));
        Assert.assertEquals("two", new ArrayList<>(result.getArray()).get(1));
        Assert.assertEquals("three", new ArrayList<>(result.getArray()).get(2));
    }

    @Test
    public void should_execute_function_split_given_single_character_and_limit() {
        Expression expression =  parseExpression("{{ split('one,two,three', ',', 2) }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        Assert.assertEquals(Type.ARRAY, result.type());
        Assert.assertEquals(2, result.getArray().size());
        Assert.assertEquals("one", new ArrayList<>(result.getArray()).get(0));
        Assert.assertEquals("two,three", new ArrayList<>(result.getArray()).get(1));
    }

    @Test
    public void should_execute_function_split_given_pattern() {
        Expression expression =  parseExpression("{{ split('one,\"two,three\",four', ',(?=([^\"]*\"[^\"]*\")*[^\"]*$)', 0) }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        Assert.assertEquals(Type.ARRAY, result.type());
        Assert.assertEquals(3, result.getArray().size());
        Assert.assertEquals("one", new ArrayList<>(result.getArray()).get(0));
        Assert.assertEquals("\"two,three\"", new ArrayList<>(result.getArray()).get(1));
        Assert.assertEquals("four", new ArrayList<>(result.getArray()).get(2));
    }
}