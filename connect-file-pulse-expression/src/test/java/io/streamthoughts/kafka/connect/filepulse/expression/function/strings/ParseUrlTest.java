/*
 * Copyright 2022 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ParseUrlTest {

    private static final StandardEvaluationContext EMPTY_CONTEXT  = new StandardEvaluationContext(new Object());

    @Test
    public void should_parse_all_uri_components_given_valid_simple_input_url() {
        Expression expression =  parseExpression("{{ parse_url('https://www.example.com') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        TypedStruct struct = result.getStruct();
        Assert.assertEquals("www.example.com", struct.getString(ParseUrl.HOST_FIELD));
        Assert.assertNull(struct.getString(ParseUrl.USER_INFO_FIELD));
        Assert.assertEquals("", struct.getString(ParseUrl.PATH_FIELD));
        Assert.assertNull(struct.getInt(ParseUrl.PORT_FIELD));
        Assert.assertNull(struct.getString(ParseUrl.QUERY_FIELD));
        Assert.assertEquals("https", struct.getString(ParseUrl.SCHEME_FIELD));
        Assert.assertNull(struct.getString(ParseUrl.FRAGMENT_FIELD));
        Assert.assertNull( struct.getMap(ParseUrl.PARAMETERS_FIELD));
    }

    @Test
    public void should_parse_all_uri_components_given_valid_complex_input_url() {
        Expression expression =  parseExpression("{{ parse_url('HTTP://USER:PASS@EXAMPLE.COM:1234/HELLO.PHP?USER=1') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        TypedStruct struct = result.getStruct();
        Assert.assertEquals("EXAMPLE.COM", struct.getString(ParseUrl.HOST_FIELD));
        Assert.assertEquals("USER:PASS", struct.getString(ParseUrl.USER_INFO_FIELD));
        Assert.assertEquals("/HELLO.PHP", struct.getString(ParseUrl.PATH_FIELD));
        Assert.assertEquals(Integer.valueOf (1234), struct.getInt(ParseUrl.PORT_FIELD));
        Assert.assertEquals("USER=1", struct.getString(ParseUrl.QUERY_FIELD));
        Assert.assertEquals("HTTP", struct.getString(ParseUrl.SCHEME_FIELD));
        Assert.assertNull(struct.getString(ParseUrl.FRAGMENT_FIELD));

        Map<Object, List<String>> parameters = struct.getMap(ParseUrl.PARAMETERS_FIELD);
        Assert.assertNotNull(parameters);
        Assert.assertEquals("1", parameters.get("USER").get(0));
    }

    @Test
    public void should_parse_all_uri_components_given_valid_input_email() {
        Expression expression =  parseExpression("{{ parse_url('mailto:abc@xyz.com') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        TypedStruct struct = result.getStruct();
        Assert.assertNull(struct.getString(ParseUrl.HOST_FIELD));
        Assert.assertEquals("abc@xyz.com", struct.getString(ParseUrl.PATH_FIELD));
        Assert.assertNull(struct.getInt(ParseUrl.PORT_FIELD));
        Assert.assertNull(struct.getString(ParseUrl.QUERY_FIELD));
        Assert.assertEquals("mailto", struct.getString(ParseUrl.SCHEME_FIELD));
        Assert.assertNull(struct.getString(ParseUrl.FRAGMENT_FIELD));
        Assert.assertNull( struct.getMap(ParseUrl.PARAMETERS_FIELD));
    }

    @Test
    public void should_return_error_given_invalid_input_uri_and_permissive_true() {
        Expression expression = parseExpression("{{ parse_url('example.com', true) }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        TypedStruct struct = result.getStruct();

        Assert.assertNotNull(struct.getString(ParseUrl.ERROR_FIELD));
        Assert.assertEquals("Could not parse URL: scheme not specified", struct.getString(ParseUrl.ERROR_FIELD));
    }


    @Test(expected = ExpressionException.class)
    public void should_return_error_given_invalid_input_uri_and_permissive_false() {
        Expression expression = parseExpression("{{ parse_url('example.com', false) }}");
        expression.readValue(EMPTY_CONTEXT, TypedValue.class);
    }
}