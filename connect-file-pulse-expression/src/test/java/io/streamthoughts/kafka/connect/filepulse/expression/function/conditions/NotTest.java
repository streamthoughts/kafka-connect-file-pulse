/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.conditions;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class NotTest {

    @Test
    public void should_return_reverse_boolean() {
        // Given
        StandardEvaluationContext context = new StandardEvaluationContext(new HashMap<>() {{
            put("boolTrue", "true");
            put("boolFalse", "false");
        }});

        // When
        TypedValue expectedFalse = parseExpression("{{ not($boolTrue) }}").readValue(context, TypedValue.class);
        TypedValue expectedTrue = parseExpression("{{ not($boolFalse) }}").readValue(context, TypedValue.class);

        // THEN
        Assert.assertFalse(expectedFalse.value());
        Assert.assertTrue(expectedTrue.value());
    }
}