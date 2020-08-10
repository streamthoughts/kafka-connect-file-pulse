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

import io.streamthoughts.kafka.connect.filepulse.config.AppendFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AppendFilterTest {

    private final static TypedStruct STRUCT = TypedStruct.create().put("values", Arrays.asList("foo", "bar"));
    private final static String EXPRESSION = "{{ extract_array(values,0) }}-{{ extract_array(values,1) }}";

    private AppendFilter filter;

    private FilterContext context;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        filter = new AppendFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new SourceMetadata("", "", 0L, 0L, 0L, -1L))
                .withOffset(FileRecordOffset.invalid())
                .build();
    }

    @Test
    public void shouldSupportSubstitutionExpressionForValueConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "target");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, EXPRESSION);
        filter.configure(configs);

        RecordsIterable<TypedStruct> output = filter.apply(context, STRUCT);
        Assert.assertNotNull(output);
        TypedStruct result = output.collect().get(0);
        Assert.assertEquals("foo-bar", result.getString("target"));
    }

    @Test
    public void shouldSupportPropertyExpressionForFieldConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$value.target");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, EXPRESSION);
        filter.configure(configs);

        RecordsIterable<TypedStruct> output = filter.apply(context, STRUCT);
        Assert.assertNotNull(output);
        TypedStruct result = output.collect().get(0);
        Assert.assertEquals("foo-bar", result.getString("target"));
    }

    @Test
    public void shouldSupportSubstitutionExpressionForFieldConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "{{ extract_array(values,0) }}");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, EXPRESSION);
        filter.configure(configs);

        RecordsIterable<TypedStruct> output = filter.apply(context, STRUCT);
        Assert.assertNotNull(output);
        TypedStruct result = output.collect().get(0);
        Assert.assertEquals("foo-bar", result.getString("foo"));
    }

    @Test
    public void shouldSupportPropertyExpressionWithScopeForFieldConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$topic");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, "my-topic-{{ extract_array(values,0) }}");
        filter.configure(configs);
        filter.apply(context, STRUCT);
        Assert.assertEquals("my-topic-foo", context.topic());
    }
}