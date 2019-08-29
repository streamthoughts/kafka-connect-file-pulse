/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function.impl;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class StartsWithTest {

    private static final String DEFAULT_TEST_VALUE = "prefix-val";

    private final StartsWith startsWith = new StartsWith();

    @Test
    public void shouldFailedGivenNoArgument() {
        SimpleArguments arguments = startsWith.prepare(new TypedValue[]{});
        assertFalse(arguments.valid());
    }

    @Test
    public void shouldAcceptGivenStringSchemaAndValue() {
        Assert.assertTrue(startsWith.accept(TypedValue.string(DEFAULT_TEST_VALUE)));
    }

    @Test
    public void shouldApplyGivenValidArgument() {
        SimpleArguments arguments = startsWith.prepare(new TypedValue[]{TypedValue.any("prefix")});
        TypedValue output = startsWith.apply(TypedValue.string(DEFAULT_TEST_VALUE), arguments);
        assertEquals(Type.BOOLEAN, output.type());
        assertTrue(output.value());
    }

}