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
package io.streamthoughts.kafka.connect.filepulse.expression.function.impl;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExistsTest {

    private static final Schema SCHEMA = SchemaBuilder
        .struct()
        .field("field", Schema.STRING_SCHEMA)
        .build();

    private static final TypedValue DEFAULT_ARGUMENT = TypedValue.any("field");

    private final Exists exists = new Exists();

    @Test
    public void shouldAcceptGivenStringSchemaAndValue() {
        Assert.assertTrue(exists.accept(TypedValue.struct(new TypedStruct())));
    }

    @Test
    public void shouldReturnFalseGivenEmptyStruct() {
        SimpleArguments arguments = exists.prepare(new TypedValue[]{DEFAULT_ARGUMENT});

        TypedValue output = exists.apply(TypedValue.struct(new TypedStruct()), arguments);
        assertEquals(Type.BOOLEAN, output.type());
        assertFalse(output.value());
    }

    @Test
    public void shouldReturnTrueGivenStructWithExpectedField() {
        SimpleArguments arguments = exists.prepare(new TypedValue[]{DEFAULT_ARGUMENT});

        TypedStruct struct = new TypedStruct()
                .put("field", TypedValue.any(null));

        TypedValue output = exists.apply(TypedValue.struct(struct), arguments);
        assertEquals(Type.BOOLEAN, output.type());
        assertTrue(output.value());
    }
}