/*
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
import io.streamthoughts.kafka.connect.filepulse.expression.SimpleExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import org.junit.Assert;
import org.junit.Test;

public class RegexExpressionParserTest {

    @Test
    public void testGivenExpressionWithStaticValue() {
        String originalExpression = "my-simple-value";
        Expression expression = new RegexExpressionParser().parseExpression(originalExpression);

        Assert.assertEquals(ValueExpression.class, expression.getClass());
        Assert.assertEquals(originalExpression, expression.originalExpression());
        Assert.assertEquals(originalExpression, expression.evaluate(null).value());
    }

    @Test
    public void testGivenSubstitutionExpressionWithRootAccess() {
        SimpleExpression expression = new RegexExpressionParser().parseSubstitutionExpression("{{ myField }}");
        Assert.assertEquals(
            "[original={{ myField }}, rootObject=myField, attribute=null, function=null]",
            expression.toString());
    }

    @Test
    public void testGivenExpressionWithRootAttributeAccess() {
        final String originalExpression = "{{ rootObject.attribute }}";
        SimpleExpression expression = new RegexExpressionParser().parseSubstitutionExpression(originalExpression);
        Assert.assertEquals(
        "[original={{ rootObject.attribute }}, rootObject=rootObject, attribute=attribute, function=null]",
        expression.toString());
    }

    @Test
    public void testGivenExpressionWithMethodAndNoArg() {
        final String originalExpression = "{{ lowercase(rootObject.attribute) }}";
        SimpleExpression expression = new RegexExpressionParser().parseSubstitutionExpression(originalExpression);
        Assert.assertEquals(
           "[original={{ lowercase(rootObject.attribute) }}, rootObject=rootObject, attribute=attribute, function=[name='lowercase', arguments=[]]]",
            expression.toString());
    }


    @Test
    public void testGivenExpressionWithRootMethodAndSingleArg() {
        final String originalExpression = "{{ extract_array(root,0) }}";
        SimpleExpression expression = new RegexExpressionParser().parseSubstitutionExpression(originalExpression);
        Assert.assertEquals(
                "[original={{ extract_array(root,0) }}, rootObject=root, attribute=null, function=[name='extract_array', arguments=[{name='index', value=0, errorMessages=[]}]]]",
                expression.toString());
    }
}