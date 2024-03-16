/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.AppendFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AppendFilterTest {

    private final static TypedStruct STRUCT = TypedStruct.create().put("values", Arrays.asList("foo", "bar"));
    private final static String EXPRESSION = "{{ extract_array($.values,0) }}-{{ extract_array($.values,1) }}";

    private AppendFilter filter;

    private FilterContext context;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        filter = new AppendFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new GenericFileObjectMeta(null, "", 0L, 0L, null, null))
                .withOffset(FileRecordOffset.invalid())
                .build();
    }

    @Test
    public void shouldSupportPropertyExpressionForValueConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$.target");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, EXPRESSION);
        filter.configure(configs, alias -> null);

        RecordsIterable<TypedStruct> output = filter.apply(context, STRUCT);
        Assert.assertNotNull(output);
        TypedStruct result = output.collect().get(0);
        Assert.assertEquals("foo-bar", result.getString("target"));
    }

    @Test
    public void shouldSupportPropertyExpressionForFieldConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$.target");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, EXPRESSION);
        filter.configure(configs, alias -> null);

        RecordsIterable<TypedStruct> output = filter.apply(context, STRUCT);
        Assert.assertNotNull(output);
        TypedStruct result = output.collect().get(0);
        Assert.assertEquals("foo-bar", result.getString("target"));
    }

    @Test
    public void shouldSupportSubstitutionExpressionForFieldConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "{{ '$.'extract_array($.values,0) }}");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, EXPRESSION);
        filter.configure(configs, alias -> null);

        RecordsIterable<TypedStruct> output = filter.apply(context, STRUCT);
        Assert.assertNotNull(output);
        TypedStruct result = output.collect().get(0);
        Assert.assertEquals("foo-bar", result.getString("foo"));
    }

    @Test
    public void shouldSupportPropertyExpressionWithScopeForFieldConfig() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$topic");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, "my-topic-{{ extract_array($.values,0) }}");
        filter.configure(configs, alias -> null);
        filter.apply(context, STRUCT);
        Assert.assertEquals("my-topic-foo", context.topic());
    }

    @Test
    public void shouldOverwriteExistingValueGivenOverwriteTrue() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$value.field");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, "bar");
        configs.put(AppendFilterConfig.APPEND_OVERWRITE_CONFIG, "true");
        filter.configure(configs, alias -> null);
        final TypedStruct input = TypedStruct.create().put("field", "foo");
        RecordsIterable<TypedStruct> results = filter.apply(context, input, false);
        Assert.assertEquals("bar", results.last().getString("field"));
    }

    @Test
    public void shouldMergeExistingValueGivenOverwriteFalse() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$value.field");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, "bar");
        configs.put(AppendFilterConfig.APPEND_OVERWRITE_CONFIG, "false");
        filter.configure(configs, alias -> null);
        final TypedStruct input = TypedStruct.create().put("field", "foo");
        RecordsIterable<TypedStruct> results = filter.apply(context, input, false);
        Assert.assertEquals("[foo, bar]", results.last().getArray("field").toString());
    }

    @Test
    public void shouldEmptyFieldGivenNullExpression() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$value.field");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, "{{ null }}");
        configs.put(AppendFilterConfig.APPEND_OVERWRITE_CONFIG, "true");
        filter.configure(configs, alias -> null);
        final TypedStruct input = TypedStruct.create().put("field", "foo");
        RecordsIterable<TypedStruct> results = filter.apply(context, input, false);
        Assert.assertNull(results.last().get("field").value());
    }

    @Test
    public void shouldEmptyValueGivenNullExpression() {
        configs.put(AppendFilterConfig.APPEND_FIELD_CONFIG, "$value");
        configs.put(AppendFilterConfig.APPEND_VALUE_CONFIG, "{{ null }}");
        configs.put(AppendFilterConfig.APPEND_OVERWRITE_CONFIG, "true");
        filter.configure(configs, alias -> null);
        final TypedStruct input = TypedStruct.create().put("field", "foo");
        RecordsIterable<TypedStruct> results = filter.apply(context, input, false);
        Assert.assertNull(results.last());
    }
}