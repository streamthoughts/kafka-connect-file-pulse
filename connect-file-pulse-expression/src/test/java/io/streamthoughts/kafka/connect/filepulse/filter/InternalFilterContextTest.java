/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
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
                .withMetadata(new GenericFileObjectMeta(null, "fileName", 0L, 1234L, null, null))
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