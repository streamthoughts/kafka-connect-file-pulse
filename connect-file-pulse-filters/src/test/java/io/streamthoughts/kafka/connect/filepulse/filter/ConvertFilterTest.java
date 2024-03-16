/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.ConvertFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConvertFilterTest {

    private ConvertFilter filter;

    private FilterContext context;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        filter = new ConvertFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new GenericFileObjectMeta(null, "", 0L, 0L, null, null))
                .withOffset(FileRecordOffset.invalid())
                .build();
    }

    @Test
    public void should_convert_value_given_valid_field() {
        configs.put(ConvertFilterConfig.CONVERT_FIELD_CONFIG, "field");
        configs.put(ConvertFilterConfig.CONVERT_TO_CONFIG, "boolean");
        filter.configure(configs, alias -> null);

        TypedStruct struct = TypedStruct.create().put("field", "yes");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        final TypedValue result = results.get(0).get("field");
        Assert.assertEquals(Type.BOOLEAN, result.type());
        Assert.assertTrue(result.getBool());
    }

    @Test
    public void should_convert_value_given_valid_path() {
        configs.put(ConvertFilterConfig.CONVERT_FIELD_CONFIG, "field.child");
        configs.put(ConvertFilterConfig.CONVERT_TO_CONFIG, "boolean");
        filter.configure(configs, alias -> null);

        TypedStruct struct = TypedStruct.create().insert("field.child", "yes");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        final TypedValue result = results.get(0).find("field.child");
        Assert.assertEquals(Type.BOOLEAN, result.type());
        Assert.assertTrue(result.getBool());
    }

    @Test(expected = FilterException.class)
    public void should_fail_given_invalid_path_and_ignore_missing_false() {
        configs.put(ConvertFilterConfig.CONVERT_FIELD_CONFIG, "field");
        configs.put(ConvertFilterConfig.CONVERT_TO_CONFIG, "boolean");
        configs.put(ConvertFilterConfig.CONVERT_IGNORE_MISSING_CONFIG, "false");
        filter.configure(configs, alias -> null);
        filter.apply(context, TypedStruct.create(), false).collect();
    }

    @Test(expected = FilterException.class)
    public void should_fail_given_not_convertible_value_and_not_default() {
        configs.put(ConvertFilterConfig.CONVERT_FIELD_CONFIG, "field");
        configs.put(ConvertFilterConfig.CONVERT_TO_CONFIG, "integer");
        configs.put(ConvertFilterConfig.CONVERT_IGNORE_MISSING_CONFIG, "false");
        filter.configure(configs, alias -> null);

        TypedStruct struct = TypedStruct.create().insert("field", "dummy");
        filter.apply(context, struct, false).collect();
    }

    @Test
    public void should_use_default_given_not_convertible_value() {
        configs.put(ConvertFilterConfig.CONVERT_FIELD_CONFIG, "field");
        configs.put(ConvertFilterConfig.CONVERT_TO_CONFIG, "integer");
        configs.put(ConvertFilterConfig.CONVERT_DEFAULT_CONFIG, "-1");
        configs.put(ConvertFilterConfig.CONVERT_IGNORE_MISSING_CONFIG, "false");
        filter.configure(configs, alias -> null);

        TypedStruct struct = TypedStruct.create().insert("field", "dummy");
        List<TypedStruct> results = filter.apply(context, struct, false).collect();
        final TypedValue result = results.get(0).find("field");
        Assert.assertEquals(Type.INTEGER, result.type());
        Assert.assertEquals(-1, result.getInt().intValue());
    }
}