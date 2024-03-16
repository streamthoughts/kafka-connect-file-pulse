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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class ToTimestampTest {

    private static final ZoneId ZID = ZoneId.systemDefault();

    @Test
    public void should_parse_given_local_date() {
        // Given
        Expression expression =  parseExpression("{{ to_timestamp($date, 'yyyy-MM-dd') }}");
        StandardEvaluationContext context = new StandardEvaluationContext(new HashMap<>() {{
            put("date", "2021-04-01");
        }});

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        final ZonedDateTime zdt = ZonedDateTime.of(2021, 4, 1, 0, 0, 0, 0, ZID);
        Assert.assertEquals(zdt.toInstant().toEpochMilli(), result.getLong().longValue());
    }

    @Test
    public void should_parse_given_local_date_with_hour() {
        // Given
        Expression expression =  parseExpression("{{ to_timestamp($date, 'yyyy-MM-dd HH') }}");
        StandardEvaluationContext context = new StandardEvaluationContext(new HashMap<>() {{
            put("date", "2021-04-01 10");
        }});

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        final ZonedDateTime zdt = ZonedDateTime.of(2021, 4, 1, 10, 0, 0, 0, ZID);
        Assert.assertEquals(zdt.toInstant().toEpochMilli(), result.getLong().longValue());
    }

    @Test
    public void should_parse_given_local_date_with_hour_and_zid() {
        // Given
        Expression expression =  parseExpression("{{ to_timestamp($date, 'yyyy-MM-dd HH', 'America/New_York') }}");
        StandardEvaluationContext context = new StandardEvaluationContext(new HashMap<>() {{
            put("date", "2021-04-01 10");
        }});

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        final ZonedDateTime zdt = ZonedDateTime.of(2021, 4, 1, 10, 0, 0, 0, ZID)
                .withZoneSameLocal(ZoneId.of("America/New_York"));
        Assert.assertEquals(zdt.toInstant().toEpochMilli(), result.getLong().longValue());
    }

}