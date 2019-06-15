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

import io.streamthoughts.kafka.connect.filepulse.data.TypeValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class StartsWithTest {

    private static final SchemaAndValue SCHEMA_AND_VALUE = new SchemaAndValue(Schema.STRING_SCHEMA, "prefix-value");
    private final StartsWith startsWith = new StartsWith();

    @Test
    public void shouldFailedGivenNoArgument() {
        SimpleArguments arguments = startsWith.prepare(new TypeValue[]{});
        assertFalse(arguments.valid());
    }

    @Test
    public void shouldAcceptGivenStringSchemaAndValue() {
        Assert.assertTrue(startsWith.accept(SCHEMA_AND_VALUE));
    }

    @Test
    public void shouldApplyGivenValidArgument() {
        SimpleArguments arguments = startsWith.prepare(new TypeValue[]{TypeValue.of("prefix")});
        SchemaAndValue output = startsWith.apply(SCHEMA_AND_VALUE, arguments);
        assertEquals(Schema.BOOLEAN_SCHEMA, output.schema());
        assertTrue((boolean)output.value());
    }

}