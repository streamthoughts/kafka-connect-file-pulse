/*
 * Copyright 2021 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NullValueFilterTest {

    private NullValueFilter filter;
    private Map<String, String> configs;
    private FilterContext context;

    @Before
    public void setUp() {
        filter = new NullValueFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new GenericFileObjectMeta(null, "", 0L, 0L, null, null))
                .withOffset(FileRecordOffset.invalid())
                .build();
    }

    @Test
    public void should_set_record_value_to_null_given_true_condition() {
        configs.put(CommonFilterConfig.CONDITION_CONFIG, "{{ $value.apply }}");
        filter.configure(configs, alias -> null);
        final TypedStruct input = TypedStruct.create().put("apply", true);
        RecordsIterable<TypedStruct> results = filter.apply(context, input, false);
        Assert.assertNull(results.last());
    }

    @Test
    public void should_not_set_record_value_to_null_given_false_condition() {
        configs.put(CommonFilterConfig.CONDITION_CONFIG, "{{ $value.apply }}");
        filter.configure(configs, alias -> null);
        final TypedStruct input = TypedStruct.create().put("apply", false);
        RecordsIterable<TypedStruct> results = filter.apply(context, input, false);
        Assert.assertNotNull(results.last());
    }
}