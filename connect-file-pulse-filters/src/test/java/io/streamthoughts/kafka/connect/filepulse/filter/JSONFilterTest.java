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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import static io.streamthoughts.kafka.connect.filepulse.data.TypedStruct.*;

public class JSONFilterTest {

    private static final String JSON = "    {\"firstName\" : \"foo\", \"lastName\" : \"bar\"}";

    private static final TypedStruct STRING_RECORD = create().put("message", JSON);
    private static final TypedStruct BYTES_RECORD = create().put("message", JSON.getBytes(StandardCharsets.UTF_8));

    private JSONFilter filter;

    @Before
    public void setUp() {
        filter = new JSONFilter();
        filter.configure(new HashMap<>());
    }

    @Test
    public void should_parse_input_given_no_target_config() {
        assertOutput(filter.apply(null, STRING_RECORD, false).collect());
    }

    @Test
    public void should_parse_input_given_target_config() {
        assertOutput(filter.apply(null, STRING_RECORD, false).collect());
    }

    @Test
    public void should_parse_input_given_value_of_type_bytes() {
        assertOutput(filter.apply(null, BYTES_RECORD, false).collect());
    }

    private void assertOutput(List<TypedStruct> output) {
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("foo", output.get(0).getString("firstName"));
        Assert.assertEquals("bar", output.get(0).getString("lastName"));
    }
}