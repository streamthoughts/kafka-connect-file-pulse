/*
 * Copyright 2023 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.data;

import java.net.URI;
import org.junit.Assert;
import org.junit.Test;

public class TypedValueTest {

    @Test
    public void should_parse_type_given_boolean_string() {
        TypedValue parsed = TypedValue.parse("true");
        Assert.assertEquals(Type.BOOLEAN, parsed.type());
        Assert.assertTrue(parsed.getBool());
    }

    @Test
    public void should_parse_type_given_numeric_string() {
        TypedValue parsed = TypedValue.parse(((Long) Long.MAX_VALUE).toString());
        Assert.assertEquals(Type.LONG, parsed.type());
        Assert.assertEquals(Long.MAX_VALUE, parsed.getLong().longValue());
    }

    @Test
    public void should_parse_type_given_too_long_numeric_string() {
        TypedValue parsed = TypedValue.parse("12345678901234567890");
        Assert.assertEquals(Type.STRING, parsed.type());
        Assert.assertEquals("12345678901234567890",  parsed.value());
    }

    @Test
    public void should_default_any_as_string_given_non_standard_type() {
        TypedValue any = TypedValue.any(URI.create("file:/tmp/test.tx"));
        Assert.assertEquals(Type.STRING, any.type());
        Assert.assertEquals("file:/tmp/test.tx", any.value());
    }
}