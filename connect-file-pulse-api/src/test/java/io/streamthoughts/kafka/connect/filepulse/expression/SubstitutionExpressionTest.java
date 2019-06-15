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
package io.streamthoughts.kafka.connect.filepulse.expression;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class SubstitutionExpressionTest {

    private static final String DEFAULT_EXPRESSION_STRING = "{{ value }}";
    private static final String SUFFIX = "-suffix";
    private static final String PREFIX = "prefix-";

    private static EvaluationContext mockEvaluationContext = Mockito.mock(EvaluationContext.class);

    private static final String EVALUATED_VALUE = "value";

    private static Expression DEFAULT_EXPRESSION = new ValueExpression(EVALUATED_VALUE);

    @Test
    public void testGivenSingleExpressionReplacement() {
        SubstitutionExpression expression = new SubstitutionExpression(DEFAULT_EXPRESSION_STRING, 0, DEFAULT_EXPRESSION_STRING.length(), DEFAULT_EXPRESSION);

        SchemaAndValue evaluated = expression.evaluate(mockEvaluationContext);

        assertNotNull(evaluated);
        assertEquals(Schema.STRING_SCHEMA, evaluated.schema());
        assertEquals(EVALUATED_VALUE, evaluated.value());
    }

    @Test
    public void testGivenPrefixedExpressionWithReplacement() {
        final int startIndex = PREFIX.length();
        SubstitutionExpression expression = new SubstitutionExpression(PREFIX + DEFAULT_EXPRESSION_STRING,
                startIndex,
                startIndex + DEFAULT_EXPRESSION_STRING.length(),
                DEFAULT_EXPRESSION);

        SchemaAndValue evaluated = expression.evaluate(mockEvaluationContext);

        assertNotNull(evaluated);
        assertEquals(Schema.STRING_SCHEMA, evaluated.schema());
        assertEquals(PREFIX + EVALUATED_VALUE, evaluated.value());
    }

    @Test
    public void testGivenSuffixedExpressionWithReplacement() {
        final int startIndex = 0;
        final int endIndex = DEFAULT_EXPRESSION_STRING.length();
        SubstitutionExpression expression = new SubstitutionExpression(DEFAULT_EXPRESSION_STRING + SUFFIX, startIndex, endIndex, DEFAULT_EXPRESSION);

        SchemaAndValue evaluated = expression.evaluate(mockEvaluationContext);

        assertNotNull(evaluated);
        assertEquals(Schema.STRING_SCHEMA, evaluated.schema());
        assertEquals(EVALUATED_VALUE + SUFFIX, evaluated.value());
    }

    @Test
    public void testGivenBetweenExpressionWithReplacement() {
        final String original = PREFIX + DEFAULT_EXPRESSION_STRING + SUFFIX;
        SubstitutionExpression expression = new SubstitutionExpression(original, PREFIX.length(), PREFIX.length() + DEFAULT_EXPRESSION_STRING.length(), DEFAULT_EXPRESSION);

        SchemaAndValue evaluated = expression.evaluate(mockEvaluationContext);

        assertNotNull(evaluated);
        assertEquals(Schema.STRING_SCHEMA, evaluated.schema());
        assertEquals(PREFIX + EVALUATED_VALUE + SUFFIX, evaluated.value());
    }

    @Test
    public void testGivenMultipleExpressionWithReplacement() {

    }
}