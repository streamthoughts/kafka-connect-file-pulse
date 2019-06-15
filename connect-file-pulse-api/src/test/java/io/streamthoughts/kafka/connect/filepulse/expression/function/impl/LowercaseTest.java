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

import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Assert;
import org.junit.Test;

public class LowercaseTest {

    private static final String DEFAULT_VALUE = "DEFAULT-VALUE";
    private static final SchemaAndValue SCHEMA_AND_VALUE = new SchemaAndValue(Schema.STRING_SCHEMA, DEFAULT_VALUE);

    private final Lowercase lowercase = new Lowercase();

    @Test
    public void shouldAcceptGivenStringSchemaAndValue() {
        Assert.assertTrue(lowercase.accept(SCHEMA_AND_VALUE));
    }

    @Test
    public void shouldApplyGivenValidArgument() {
        SchemaAndValue output = lowercase.apply(SCHEMA_AND_VALUE, Arguments.empty());
        Assert.assertEquals(Schema.STRING_SCHEMA, output.schema());
        Assert.assertEquals(DEFAULT_VALUE.toLowerCase(), output.value());
    }
}