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