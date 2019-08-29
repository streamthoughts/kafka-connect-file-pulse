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

public class NlvTest {

    private static final TypedValue DEFAULT_ARGUMENT = TypedValue.any("default");

    private final Nlv nlv = new Nlv();

    @Test
    public void shouldAcceptGivenNull() {
        Assert.assertTrue(nlv.accept(null));
    }

    @Test
    public void shouldReturnGivenNonNullValue() {
        SimpleArguments arguments = nlv.prepare(new TypedValue[]{DEFAULT_ARGUMENT});
        TypedValue output = nlv.apply(TypedValue.string("nonempty"), arguments);
        assertEquals(Type.STRING, output.type());
        assertEquals("nonempty", output.value());
    }

    @Test
    public void shouldReturnDefaultGivenNullValue() {
        SimpleArguments arguments = nlv.prepare(new TypedValue[]{DEFAULT_ARGUMENT});
        TypedValue output = nlv.apply(TypedValue.string(null), arguments);
        assertEquals(Type.STRING, output.type());
        assertEquals("default", output.value());
    }
}