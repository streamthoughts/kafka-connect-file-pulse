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
package io.streamthoughts.kafka.connect.filepulse.expression.parser.antlr4;

import static org.junit.Assert.assertEquals;

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.FunctionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.PropertyExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.SubstitutionExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class Antlr4ExpressionParserTest {

    @Test
    public void should_parse_value_expression_given_string_literal() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("'hello'");
        Assert.assertTrue(expression instanceof ValueExpression);
        Assert.assertEquals("'hello'", expression.originalExpression());
        Assert.assertEquals("hello", ((ValueExpression)expression).value().getString());
    }

    @Test
    public void should_parse_value_expression_given_boolean_literal() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("true");
        Assert.assertTrue(expression instanceof ValueExpression);
        Assert.assertEquals("true", expression.originalExpression());
    }

    @Test
    public void should_parse_value_expression_given_null_literal() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("null");
        Assert.assertEquals("null", expression.originalExpression());
        Assert.assertTrue(expression instanceof ValueExpression);
        Assert.assertTrue(((ValueExpression) expression).value().isNull());
    }

    @Test
    public void should_parse_value_expression_given_integer_literal() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("0");
        Assert.assertTrue(expression instanceof ValueExpression);
        Assert.assertEquals("0", expression.originalExpression());
    }

    @Test
    public void should_parse_substitution_expression_given_property_with_scope() {
        final String strExpression = "{{ $scope }}";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertSubstitutionException(
            expression,
            strExpression.replaceAll("\"",""),
            "[startIndex=0, endIndex=12, expressions=[[originalExpression=$scope, rootObject=scope, attribute=null]]]"
        );
    }

    @Test
    public void should_parse_substitution_expression_containing_literal_and_function() {
        final String strExpression = "{{ '$.'lowercase($.field) }}";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertSubstitutionException(
            expression,
            strExpression.replaceAll("\"",""),
            "[startIndex=0, endIndex=28, expressions=[$., [originalExpression=lowercase($.field), function=[name='lowercase', arguments=[{name='field_expr', value=[originalExpression=$.field, rootObject=value, attribute=field]}]]]]]"
        );
    }

    @Test
    public void should_parse_substitution_expression_given_property_with_scope_an_space() {
        final String strExpression = "prefix-{{ $scope }}";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertSubstitutionException(
            expression,
            strExpression.replaceAll("\"",""),
            "[startIndex=7, endIndex=19, expressions=[[originalExpression=$scope, rootObject=scope, attribute=null]]]"
        );
    }

    @Test
    public void should_parse_multiple_substitution_expression_given_property_with_scope() {
        final String strExpression = "prefix-{{ $scope.field1   }}-{{ $scope.field2 }}-suffix";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertSubstitutionException(
            expression,
            strExpression.replaceAll("\"",""),
            "[startIndex=7, endIndex=28, expressions=[[originalExpression=$scope.field1, rootObject=scope, attribute=field1]]]",
            "[startIndex=29, endIndex=48, expressions=[[originalExpression=$scope.field2, rootObject=scope, attribute=field2]]]"
        );
    }

    @Test
    public void should_parse_substitution_expression_given_function_with_property() {
        final String strExpression = "{{ lowercase($scope) }}";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertSubstitutionException(
            expression,
            strExpression.replaceAll("\"",""),
            "[startIndex=0, endIndex=23, expressions=[[originalExpression=lowercase($scope), function=[name='lowercase', arguments=[{name='field_expr', value=[originalExpression=$scope, rootObject=scope, attribute=null]}]]]]]"
        );
    }

    @Test
    public void should_parse_substitution_expression_given_multiple_functions_with_property_and_args() {
        final String strExpression = "{{ extract_array($.values,0)}}-{{extract_array($.values,1) }}";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertSubstitutionException(
            expression,
            strExpression.replaceAll("\"",""),
            "[startIndex=0, endIndex=30, expressions=[[originalExpression=extract_array($.values,0), function=[name='extract_array', arguments=[{name='array', value=[originalExpression=$.values, rootObject=value, attribute=values]}, {name='index', value=0}]]]]]",
            "[startIndex=31, endIndex=61, expressions=[[originalExpression=extract_array($.values,1), function=[name='extract_array', arguments=[{name='array', value=[originalExpression=$.values, rootObject=value, attribute=values]}, {name='index', value=1}]]]]]"
        );
    }

    @Test
    public void should_parse_property_expression_given_property_with_scope() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("$scope");
        assertPropertyException(
            expression,
            "$scope",
            "scope",
            "null"
        );
    }

    @Test
    public void should_parse_property_expression_given_property_with_scope_and_attr() {
        final String strExpression = "$scope.field.two";

        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertPropertyException(
            expression,
            strExpression,
            "scope",
            "field.two"
        );
    }

    @Test
    public void should_parse_property_expression_given_property_with_default_scope() {
        final String strExpression = "$.field.two";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);
        assertPropertyException(
            expression,
            strExpression,
            "value",
            "field.two"
        );
    }

    @Test
    public void should_parse_function_expression_given_property() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("{{lowercase($scope.attr) }}");

        assertFunctionException(
            expression,
            "lowercase($scope.attr)",
            "[name='lowercase', arguments=[{name='field_expr', value=[originalExpression=$scope.attr, rootObject=scope, attribute=attr]}]]"
        );
    }

    @Test
    public void should_parse_function_expression_given_property_and_args() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("{{ starts_with($scope.attr, 'foo') }}");

        assertFunctionException(
            expression,
            "starts_with($scope.attr,'foo')",
            "[name='starts_with', arguments=[{name='field_expr', value=[originalExpression=$scope.attr, rootObject=scope, attribute=attr]}, {name='prefix', value=foo}]]"
        );
    }

    @Test
    public void should_parse_function_expression_given_nested_function_with_single_arg() {
        Expression expression = new Antlr4ExpressionParser().parseExpression("{{ replace_all(lowercase($.field),'\\\\s', '-') }}");

        assertFunctionException(
            expression,
            "replace_all(lowercase($.field),'\\\\s','-')",
            "[name='replace_all', arguments=[{name='field_expr', value=[originalExpression=lowercase($.field), function=[name='lowercase', arguments=[{name='field_expr', value=[originalExpression=$.field, rootObject=value, attribute=field]}]]]}, {name='pattern', value=\\\\s}, {name='replacement', value=-}]]"
        );
    }

    @Test
    public void should_parse_function_expression_given_nested_function_with_multiple_args() {
        final String strExpression = "{{ lowercase( extract_array($.values,0) ) }}";
        Expression expression = new Antlr4ExpressionParser().parseExpression(strExpression);

        assertFunctionException(
            expression,
            "lowercase(extract_array($.values,0))",
            "[name='lowercase', arguments=[{name='field_expr', value=[originalExpression=extract_array($.values,0), function=[name='extract_array', arguments=[{name='array', value=[originalExpression=$.values, rootObject=value, attribute=values]}, {name='index', value=0}]]]}]]"
        );
    }

    private static void assertSubstitutionException(final Expression expression,
                                                    final String originalExpression,
                                                    final String... replacements) {
        Assert.assertTrue(expression instanceof SubstitutionExpression);
        assertEquals(originalExpression, expression.originalExpression());

        List<SubstitutionExpression.ReplacementExpression> replacementExpressions
                = new ArrayList<>(((SubstitutionExpression) expression).getReplacements());

        for (int i = 0; i <  replacementExpressions.size(); i++) {
            assertEquals(replacements[i], replacementExpressions.get(i).toString());
        }
    }

    private static void assertPropertyException(final Expression expression,
                                                final String originalExpression,
                                                final String rootObject,
                                                final String attribute) {
        Assert.assertTrue(expression instanceof PropertyExpression);
        String expected =
            "[" +
            "originalExpression=" + originalExpression + ", " +
            "rootObject=" + rootObject + ", " +
            "attribute=" + attribute +
            "]";
        assertEquals(expected, expression.toString());
    }

    private static void assertFunctionException(Expression expression,
                                                final String originalExpression,
                                                final String function) {

        if (expression instanceof SubstitutionExpression) {
            expression = ((SubstitutionExpression) expression).getReplacements().first().expressions().get(0);
        }
        Assert.assertTrue(expression instanceof FunctionExpression);
        assertEquals("originalExpression", originalExpression, expression.originalExpression());
        assertEquals("function", function, ((FunctionExpression)expression).getFunctionExecutor().toString());
    }
}