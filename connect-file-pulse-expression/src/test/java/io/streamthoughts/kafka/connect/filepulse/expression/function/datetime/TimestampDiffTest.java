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
package io.streamthoughts.kafka.connect.filepulse.expression.function.datetime;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.Assert;
import org.junit.Test;

public class TimestampDiffTest {

    @Test
    public void should_get_diff_in_seconds_given_epoch_time_in_millis() {
        // Given

        final Instant now = Instant.now();
        final Instant minus = now.minusSeconds(10);
        Expression expression = buildExpressionFor(now.toEpochMilli(), minus.toEpochMilli(), ChronoUnit.SECONDS);
        StandardEvaluationContext context = new StandardEvaluationContext(new Object());

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        Assert.assertEquals(10, result.getLong().longValue());
    }

    @Test
    public void should_get_diff_in_millis_given_epoch_time_in_millis() {
        // Given

        final Instant now = Instant.now();
        final Instant minus = now.minusSeconds(10);
        Expression expression = buildExpressionFor(now.toEpochMilli(), minus.toEpochMilli(), ChronoUnit.MILLIS);
        StandardEvaluationContext context = new StandardEvaluationContext(new Object());

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        Assert.assertEquals(10_000, result.getLong().longValue());
    }

    @Test
    public void should_get_diff_in_millis_given_epoch_time_in_seconds() {
        // Given

        final Instant now = Instant.now();
        final Instant minus = now.minusSeconds(10);
        Expression expression = buildExpressionFor(now.toEpochMilli(), minus.getEpochSecond(), ChronoUnit.MILLIS);
        StandardEvaluationContext context = new StandardEvaluationContext(new Object());

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        Assert.assertEquals(10_000, result.getLong().longValue());
    }

    public Expression buildExpressionFor(final Long epochTimeLeft,
                                         final Long epochTimeRight,
                                         final ChronoUnit unit) {
        return parseExpression("{{ timestamp_diff('" + unit + "', " + epochTimeLeft + ", " + epochTimeRight + ") }}");
    }
}