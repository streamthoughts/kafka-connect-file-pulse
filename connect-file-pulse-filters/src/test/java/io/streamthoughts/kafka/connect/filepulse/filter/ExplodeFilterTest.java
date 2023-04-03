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

import static io.streamthoughts.kafka.connect.filepulse.data.TypedStruct.create;

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExplodeFilterTest {

    private static final List<String> PRIMITIVE_VALUES;
    private static final List<TypedStruct> COMPLEXES_VALUES;

    static {
        PRIMITIVE_VALUES = Arrays.asList("one", "two", "three", "four");
        COMPLEXES_VALUES = PRIMITIVE_VALUES.stream()
            .map(it ->  TypedStruct.create().put("field", it))
            .collect(Collectors.toList());
    }
    private ExplodeFilter filter;

    @Before
    public void setUp() {
        filter = new ExplodeFilter();
        filter.configure(new HashMap<>());
    }

    @Test
    public void should_explode_input_given_array_of_primitive() {
        RecordsIterable<TypedStruct> result = filter.apply(null, create().put("message", PRIMITIVE_VALUES), false);
        Assert.assertEquals(PRIMITIVE_VALUES.size(), result.size());
        Assert.assertEquals(PRIMITIVE_VALUES, extractStringValues(result, it -> it.getString("message")));
    }

    @Test
    public void should_explode_input_given_array_of_complex() {
        RecordsIterable<TypedStruct> result = filter.apply(null, create().put("message", COMPLEXES_VALUES), false);
        Assert.assertEquals(COMPLEXES_VALUES.size(), result.size());
        Assert.assertEquals(COMPLEXES_VALUES, extractStringValues(result, it -> it.getStruct("message")));
    }

    @Test
    public void should_explode_input_given_sub_field_source() {
        filter.configure(new HashMap<String, String>() {{
            put(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG, "values");
        }});

        RecordsIterable<TypedStruct> result = filter.apply(null, create()
                .put("message", "dummy")
                .put("values", PRIMITIVE_VALUES), false);
        Assert.assertEquals(PRIMITIVE_VALUES.size(), result.size());
        Assert.assertEquals(PRIMITIVE_VALUES, extractStringValues(result, it -> it.getString("values")));
    }

    @Test
    public void should_explode_input_given_sub_dotted_field_source() {
        filter.configure(new HashMap<String, String>() {{
            put(CommonFilterConfig.FILTER_SOURCE_FIELD_CONFIG, "field.values");
        }});

        RecordsIterable<TypedStruct> result = filter.apply(null, create()
                .insert("message", "dummy")
                .insert("field.values", PRIMITIVE_VALUES), false);
        Assert.assertEquals(PRIMITIVE_VALUES.size(), result.size());
        Assert.assertEquals(PRIMITIVE_VALUES, extractStringValues(result, it -> it.find("field.values").getString()));
    }

    private <T> List<T> extractStringValues(final RecordsIterable<TypedStruct> result,
                                             final Function<TypedStruct, T> extractor) {
        return result.stream().map(extractor).collect(Collectors.toList());
    }
}