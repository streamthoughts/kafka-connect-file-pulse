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

public class NlvTest {

    private final Nlv nlv = new Nlv();

    @Test
    public void shouldAcceptGivenNull() {
        Assert.assertTrue(nlv.accept(null));
    }

    @Test
    public void shouldReturnGivenNonNullValue() {
        SimpleArguments arguments = nlv.prepare(new TypeValue[]{TypeValue.of("default")});
        SchemaAndValue output = nlv.apply(new SchemaAndValue(Schema.STRING_SCHEMA, "nonempty"), arguments);
        assertEquals(Schema.STRING_SCHEMA, output.schema());
        assertEquals("nonempty", output.value());
    }

    @Test
    public void shouldReturnDefaultGivenNullValue() {
        SimpleArguments arguments = nlv.prepare(new TypeValue[]{TypeValue.of("default")});
        SchemaAndValue output = nlv.apply(new SchemaAndValue(Schema.STRING_SCHEMA, null), arguments);
        assertEquals(Schema.STRING_SCHEMA, output.schema());
        assertEquals("default", output.value());
    }
}