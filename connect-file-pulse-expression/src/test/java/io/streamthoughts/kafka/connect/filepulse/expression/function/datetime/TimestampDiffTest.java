/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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