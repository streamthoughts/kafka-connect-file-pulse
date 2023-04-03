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
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class FunctionsTest {

    private static final StandardEvaluationContext EMPTY_CONTEXT  = new StandardEvaluationContext(new Object());

    @Test
    public void should_execute_uppercase_function() {
        Expression expression =  parseExpression("{{ uppercase('foo') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        Assert.assertEquals("FOO", result.value());
    }

    @Test
    public void should_execute_lowercase_function() {
        Expression expression =  parseExpression("{{ lowercase('FOO') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        Assert.assertEquals("foo", result.value());
    }

    @Test
    public void should_execute_start_with_function() {
        Expression expressionTrue = parseExpression("{{ starts_with('prefix-FOO', 'prefix') }}");
        Expression expressionFalse = parseExpression("{{ starts_with('prefix-FOO', 'bad') }}");

        Assert.assertTrue(expressionTrue.readValue(EMPTY_CONTEXT, TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_replace_all_function() {
        Expression expression = parseExpression("{{ replace_all('aaa', 'a', 'b') }}");
        Assert.assertEquals("bbb", expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_nlv_function() {
        Expression expressionNull = parseExpression("{{ nlv($nullValue, 'foo') }}");
        Expression expressionNotNull = parseExpression("{{ nlv($notNullValue, 'foo') }}");

        StandardEvaluationContext context = new StandardEvaluationContext(
            TypedStruct
                .create()
                .put("nullValue", TypedValue.string(null))
                .put("notNullValue", TypedValue.string("bar"))
        );

        Assert.assertEquals("foo", expressionNull.readValue(context, TypedValue.class).value());
        Assert.assertEquals("bar", expressionNotNull.readValue(context, TypedValue.class).value());
    }

    @Test
    public void should_execute_trim_function() {
        Expression expression = parseExpression("{{ trim('  foo  ') }}");
        Assert.assertEquals("foo", expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_matches_function() {
        Expression expression = parseExpression("{{ matches('prefix_foo', '^prefix_.*$') }}");
        Assert.assertTrue(expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_length_function_given_string() {
        Expression expression = parseExpression("{{ length('foo') }}");
        Assert.assertEquals(3, expression.readValue(EMPTY_CONTEXT, TypedValue.class).getInt().intValue());
    }

    @Test
    public void should_execute_length_function_given_array() {
        Expression expression = parseExpression("{{ length($array) }}");
        StandardEvaluationContext context = new StandardEvaluationContext(
            TypedStruct
                .create()
                .put("array", TypedValue.array(Arrays.asList("one", "two", "three"), Type.STRING))
        );
        Assert.assertEquals(3, expression.readValue(context, TypedValue.class).getInt().intValue());
    }

    @Test
    public void should_execute_is_null_function() {
        Expression expressionTrue = parseExpression("{{ is_null($nullValue) }}");
        Expression expressionFalse = parseExpression("{{ is_null($notNullValue) }}");

        StandardEvaluationContext context = new StandardEvaluationContext(
            TypedStruct
                .create()
                .put("nullValue", TypedValue.string(null))
                .put("notNullValue", TypedValue.string(""))
        );
        Assert.assertTrue(expressionTrue.readValue(context, TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(context, TypedValue.class).value());
    }

    @Test
    public void should_execute_extract_array() {
        Expression expression = parseExpression("{{ extract_array($array, 1) }}");
        StandardEvaluationContext context = new StandardEvaluationContext(
            TypedStruct
                .create()
                .put("array", TypedValue.array(Arrays.asList("one", "two", "three"), Type.STRING))
        );
        Assert.assertEquals("two", expression.readValue(context, TypedValue.class).getString());
    }

    @Test
    public void should_execute_end_with_function() {
        Expression expressionTrue = parseExpression("{{ ends_with('FOO-suffix', 'suffix') }}");
        Expression expressionFalse = parseExpression("{{ ends_with('FOO-suffix', 'bad') }}");

        Assert.assertTrue(expressionTrue.readValue(EMPTY_CONTEXT, TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_exists_function() {
        Expression expressionTrue = parseExpression("{{ exists($value, 'field') }}");
        Expression expressionFalse= parseExpression("{{ exists($value, 'none') }}");
        StandardEvaluationContext context = new StandardEvaluationContext(
            TypedStruct.create().put("value", TypedStruct.create().put("field", ""))
        );
        Assert.assertTrue(expressionTrue.readValue(context, TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(context, TypedValue.class).value());
    }

    @Test
    public void should_execute_converts_function() {
        Expression expression = parseExpression("{{ converts('true', 'BOOLEAN') }}");
        Assert.assertTrue(expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_uuid_function() {
        Expression expression = parseExpression("{{ uuid() }}");
        Assert.assertNotNull("foo", expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_concat_function() {
        Expression expression = parseExpression("{{ concat('one','two','three') }}");
        Assert.assertEquals("onetwothree", expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_concat_ws_function() {
        Expression expression = parseExpression("{{ concat_ws(',','[', ']', 'one','two','three') }}");
        Assert.assertEquals("[one,two,three]", expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_concat_ws_function_given_no_fields() {
        Expression expression = parseExpression("{{ concat_ws(',','[', ']') }}");
        Assert.assertEquals("[]", expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_concat_ws_function_given_no_empty_prefix_suffix() {
        Expression expression = parseExpression("{{ concat_ws(',','','', 'one','two','three') }}");
        Assert.assertEquals("one,two,three", expression.readValue(EMPTY_CONTEXT, TypedValue.class).value());
    }

    @Test
    public void should_execute_hash_function() {
        Expression expression = parseExpression("{{ hash('hello') }}");
        Assert.assertEquals("2132663229", expression.readValue(EMPTY_CONTEXT, TypedValue.class).getString());
    }

    @Test
    public void should_execute_md5_function() {
        Expression expression = parseExpression("{{ md5('hello') }}");
        Assert.assertEquals("5d41402abc4b2a76b9719d911017c592", expression.readValue(EMPTY_CONTEXT, TypedValue.class).getString());
    }

    @Test
    public void should_execute_if_function() {
        Expression expressionTrue =  parseExpression("{{ if(exists($value, 'field'), true, false) }}");
        Expression expressionFalse =  parseExpression("{{ if(exists($value, 'dummy'), true, false) }}");
        StandardEvaluationContext context = new StandardEvaluationContext(
            TypedStruct.create().put("value", TypedStruct.create().put("field", ""))
        );
        Assert.assertTrue(expressionTrue.readValue(context, TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(context, TypedValue.class).value());
    }

    @Test
    public void should_execute_or_function() {
        Expression expressionTrue = parseExpression("{{ or(true, false) }}");
        Expression expressionFalse = parseExpression("{{ or(false, false) }}");
        Assert.assertTrue(expressionTrue.readValue(new StandardEvaluationContext(new Object()), TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(new StandardEvaluationContext(new Object()), TypedValue.class).value());
    }

    @Test
    public void should_execute_greater_than_function() {
        Expression expressionTrue = parseExpression("{{ gt(2, 1) }}");
        Expression expressionFalse = parseExpression("{{ gt(1, 2) }}");
        Assert.assertTrue(expressionTrue.readValue(new StandardEvaluationContext(new Object()), TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(new StandardEvaluationContext(new Object()), TypedValue.class).value());
    }

    @Test
    public void should_execute_less_than_function() {
        Expression expressionTrue = parseExpression("{{ lt(1, 2) }}");
        Expression expressionFalse = parseExpression("{{ lt(2, 1) }}");
        Assert.assertTrue(expressionTrue.readValue(new StandardEvaluationContext(new Object()), TypedValue.class).value());
        Assert.assertFalse(expressionFalse.readValue(new StandardEvaluationContext(new Object()), TypedValue.class).value());
    }
}