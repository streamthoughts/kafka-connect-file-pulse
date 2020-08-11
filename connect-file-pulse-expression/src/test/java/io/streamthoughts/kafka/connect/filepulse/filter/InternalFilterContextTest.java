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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InternalFilterContextTest {

    private InternalFilterContext context;
    private StandardEvaluationContext evaluationContext;

    @Before
    public void setUp() {
        context = (InternalFilterContext) FilterContextBuilder
                .newBuilder()
                .withMetadata(new SourceMetadata("fileName", "fileName", 0L, 1234L, 1L, 1L))
                .withOffset(FileRecordOffset.invalid())
                .build();

        evaluationContext = new StandardEvaluationContext(context);
    }

    @Test
    public void shouldAllowWriteAccessToPropertyTopicWhenExpressionIsUsed() {
        Expression expression = ExpressionParsers.parseExpression("$topic");
        expression.writeValue("test-topic", evaluationContext);
        Assert.assertEquals("test-topic", context.topic());
    }

    @Test
    public void shouldAllowWriteAccessToPropertyPartitionWhenExpressionIsUsed() {
        Expression expression = ExpressionParsers.parseExpression("$partition");
        expression.writeValue(42, evaluationContext);
        Assert.assertEquals(42, context.partition().intValue());
    }

    @Test
    public void shouldAllowWriteAccessToPropertyHeadersWhenExpressionIsUsed() {
        Expression expression = ExpressionParsers.parseExpression("$headers.myHeader");
        expression.writeValue("myValue", evaluationContext);
        Assert.assertEquals("myValue", context.headers().lastWithName("myHeader").value());
    }

    @Test
    public void shouldAllowWriteAccessToPropertyVariablesWhenExpressionIsUsed() {
        Expression expression = ExpressionParsers.parseExpression("$variables.myVariable");
        expression.writeValue("myValue", evaluationContext);
        Assert.assertEquals("myValue", context.variables().get("myVariable"));
    }

}