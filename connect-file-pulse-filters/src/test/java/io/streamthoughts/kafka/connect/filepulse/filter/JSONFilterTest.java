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

import static io.streamthoughts.kafka.connect.filepulse.data.TypedStruct.*;

import io.streamthoughts.kafka.connect.filepulse.config.JSONFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.SchemaSupplier;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JSONFilterTest {

    private static final String JSON = "    {\"firstName\" : \"foo\", \"lastName\" : \"bar\"}";
    private static final String ARRAY_STRING_JSON = "[\"foo\", \"bar\"]";
    private static final String ARRAY_STRUCT_JSON = "[{\"name\" : \"foo\"}, {\"name\" : \"bar\"}]";

    private static final TypedStruct STRING_RECORD = create().put("message", JSON);
    private static final TypedStruct BYTES_RECORD = create().put("message", JSON.getBytes(StandardCharsets.UTF_8));

    private static final TypedStruct ARRAY_STRING_RECORD = create().put("message", ARRAY_STRING_JSON);
    private static final TypedStruct ARRAY_EMPTY_RECORD = create().put("message", "[]");
    private static final TypedStruct ARRAY_STRUCT_RECORD = create().put("message", ARRAY_STRUCT_JSON);

    private JSONFilter filter;

    @Before
    public void setUp() {
        filter = new JSONFilter();
        filter.configure(new HashMap<>());
    }

    @Test
    public void should_add_parsed_json_into_source_given_no_target_field() {
        List<TypedStruct> expected = Collections.singletonList(create()
                .put("message", create().put("firstName", "foo").put("lastName", "bar"))
        );
        assertOutput(filter.apply(null, STRING_RECORD, false).collect(), expected);
    }

    @Test
    public void should_add_parsed_json_into_specific_field_given_target_field() {
        filter.configure(new HashMap<String, Object>(){{
            put(JSONFilterConfig.JSON_TARGET_CONFIG, "myTarget");
        }});

        List<TypedStruct> expected = Collections.singletonList(create()
            .put("message", JSON)
            .put("myTarget", create().put("firstName", "foo").put("lastName", "bar"))
        );
        assertOutput(filter.apply(null, STRING_RECORD, false).collect(), expected);
    }

    @Test
    public void should_add_parsed_array_into_specific_field_given_target_field() {
        filter.configure(new HashMap<String, Object>(){{
            put(JSONFilterConfig.JSON_TARGET_CONFIG, "myTarget");
        }});

        List<TypedStruct> expected = Collections.singletonList(
            create().put("message", ARRAY_STRING_JSON)
                    .put("myTarget", TypedValue.array(Arrays.asList("foo", "bar"), Type.STRING))
        );

        assertOutput(filter.apply(null, ARRAY_STRING_RECORD, false).collect(), expected);
    }

    @Test
    public void should_add_parsed_array_into_specific_field_given_target_field_and_empty_array() {
        filter.configure(new HashMap<String, Object>(){{
            put(JSONFilterConfig.JSON_TARGET_CONFIG, "myTarget");
        }});

        List<TypedStruct> expected = Collections.singletonList(
                create().put("message", "[]")
                        .put("myTarget", TypedValue.array(new ArrayList<>(), SchemaSupplier.lazy(new ArrayList<>()).get()))
        );

        final List<TypedStruct> collect = filter.apply(null, ARRAY_EMPTY_RECORD, false).collect();
        assertOutput(collect, expected);
    }

    @Test
    public void should_add_parsed_array_into_specific_field_given_target_and_explode_true() {
        filter.configure(new HashMap<String, Object>(){{
            put(JSONFilterConfig.JSON_TARGET_CONFIG, "myTarget");
            put(JSONFilterConfig.JSON_EXPLODE_ARRAY_CONFIG, true);
        }});

        List<TypedStruct> expected = Arrays.asList(
            create().put("message", ARRAY_STRING_JSON).put("myTarget", "foo"),
            create().put("message", ARRAY_STRING_JSON).put("myTarget", "bar")
        );

        assertOutput(filter.apply(null, ARRAY_STRING_RECORD, false).collect(), expected);
    }

    @Test
    public void should_merge_parsed_json_into_root_field_given_merge_true() {
        filter.configure(new HashMap<String, Object>(){{
            put(JSONFilterConfig.JSON_MERGE_CONFIG, true);
        }});

        List<TypedStruct> expected = Collections.singletonList(create()
            .put("message", JSON)
            .put("firstName", "foo")
            .put("lastName", "bar")
        );
        assertOutput(filter.apply(null, STRING_RECORD, false).collect(), expected);
    }

    @Test
    public void should_merge_parsed_array_into_root_field_given_merge_and_explode_true() {
        filter.configure(new HashMap<String, Object>(){{
            put(JSONFilterConfig.JSON_MERGE_CONFIG, true);
            put(JSONFilterConfig.JSON_EXPLODE_ARRAY_CONFIG, true);
        }});

        List<TypedStruct> expected = Arrays.asList(
                create().put("message", ARRAY_STRUCT_JSON).put("name", "foo"),
                create().put("message", ARRAY_STRUCT_JSON).put("name", "bar")
        );
        assertOutput(filter.apply(null, ARRAY_STRUCT_RECORD, false).collect(), expected);
    }

    @Test
    public void should_parse_input_given_value_of_type_bytes() {
        List<TypedStruct> expected = Collections.singletonList(create()
                .put("message", create().put("firstName", "foo").put("lastName", "bar"))
        );
        assertOutput(filter.apply(null, BYTES_RECORD, false).collect(), expected);
    }

    private void assertOutput(List<TypedStruct> items, List<TypedStruct> expected) {
        Assert.assertEquals(expected.size(), items.size());
        for (int i = 0; i < items.size(); i++) {
            Assert.assertEquals(expected.get(i), items.get(i));
        }
    }
}