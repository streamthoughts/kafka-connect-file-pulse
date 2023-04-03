/*
 * Copyright 2022 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function.objects;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import java.util.HashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExtractStructFieldTest {

    public static final String TEST_VALUE = "value";

    @Test
    public void should_extract_field_from_struct() {
        // Given
        Expression expression =  parseExpression("{{ extract_struct_field($object, 'path') }}");
        StandardEvaluationContext context = new StandardEvaluationContext(new HashMap<>() {{
            put("object", TypedStruct.create().put("path", TEST_VALUE));
        }});

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        Assertions.assertEquals(TEST_VALUE, result.getString());
    }

    @Test
    public void should_return_null_given_empty_struct() {
        // Given
        Expression expression =  parseExpression("{{ extract_struct_field($object, 'path') }}");
        StandardEvaluationContext context = new StandardEvaluationContext(new HashMap<>() {{
            put("object", TypedValue.none());
        }});

        // When
        TypedValue result = expression.readValue(context, TypedValue.class);

        // THEN
        Assertions.assertNull(result);
    }

    @Test
    public void should_fail_given_non_struct_object() {
        // Given
        Expression expression =  parseExpression("{{ extract_struct_field($object, 'path') }}");
        StandardEvaluationContext context = new StandardEvaluationContext(new HashMap<>() {{
            put("object", "string");
        }});

        //  When / THEN
        Assertions.assertThrowsExactly(
                ExpressionException.class,
                () -> expression.readValue(context, TypedValue.class),
                "Expected type STRUCT, was STRING"
        );
    }
}