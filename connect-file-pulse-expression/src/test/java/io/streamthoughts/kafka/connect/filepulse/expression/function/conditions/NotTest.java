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