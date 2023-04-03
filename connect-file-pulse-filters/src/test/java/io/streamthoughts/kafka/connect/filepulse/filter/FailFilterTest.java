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

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.config.FailFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FailFilterTest {

    private static final TypedStruct DEFAULT_DATA = TypedStruct.create()
            .put("message", "simple record");

    private FilterContext context;

    private Map<String, String> configs;

    @Before
    public void setUp() {
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new GenericFileObjectMeta(null, "", 0L, 0L, null, null))
                .withOffset(FileRecordOffset.invalid())
                .build();
        configs = new HashMap<>();
        configs.put(CommonFilterConfig.CONDITION_CONFIG, "{{ exists($value, 'tags') }}");
        configs.put(CommonFilterConfig.CONDITION_NOT_CONFIG, "true");
    }

    @Test
    public void shouldThrownExceptionWhenConditionIsTrue() {
        configs.put(FailFilterConfig.MESSAGE_CONFIG, "Unexpected error");
        FailFilter filter = new FailFilter();
        filter.configure(configs, alias -> null);

        try {
            filter.apply(context, DEFAULT_DATA, false);
        } catch (final FilterException exception) {
            Assert.assertEquals("Unexpected error", exception.getMessage());
            return;
        }
        Assert.fail();
    }


    @Test
    public void shouldEvaluateMessageExpression() {
        configs.put(FailFilterConfig.MESSAGE_CONFIG, "Unexpected error : {{ $value.message }}");
        FailFilter filter = new FailFilter();
        filter.configure(configs, alias -> null);

        try {
            filter.apply(context, DEFAULT_DATA, false);
        } catch (final FilterException exception) {
            Assert.assertEquals("Unexpected error : simple record", exception.getMessage());
            return;
        }
        Assert.fail();
    }

}