/*
 * Copyright 2019-2020 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import org.junit.Assert;
import org.junit.Test;

public class LowercaseTest {

    private static final String DEFAULT_VALUE = "DEFAULT-VALUE";

    private static final TypedValue DEFAULT_TYPE_VALUE = TypedValue.string(DEFAULT_VALUE);

    private final Lowercase lowercase = new Lowercase();

    @Test
    public void shouldAcceptGivenStringSchemaAndValue() {
        Assert.assertTrue(lowercase.accept(DEFAULT_TYPE_VALUE));
    }

    @Test
    public void shouldApplyGivenValidArgument() {
        TypedValue output = lowercase.apply(DEFAULT_TYPE_VALUE, Arguments.empty());
        Assert.assertEquals(Type.STRING, output.type());
        Assert.assertEquals(DEFAULT_VALUE.toLowerCase(), output.value());
    }
}