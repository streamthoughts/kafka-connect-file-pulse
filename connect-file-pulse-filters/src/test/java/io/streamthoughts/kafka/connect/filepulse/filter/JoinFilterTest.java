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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.streamthoughts.kafka.connect.filepulse.config.JoinFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class JoinFilterTest {

    private JoinFilter filter;
    private FilterContext context;
    private Map<String, Object> configs;

    @Before
    public void setUp() {
        filter = new JoinFilter();
        configs = new HashMap<>();
        context = FilterContextBuilder.newBuilder()
                .withMetadata(new GenericFileObjectMeta(null, "", 0L, 0L, null, null))
                .withOffset(FileRecordOffset.invalid())
                .build();
    }

    @Test
    public void shouldJoinArrayIntoFieldGivenNoTarget() {
        configs.put(JoinFilterConfig.JOIN_FIELD_CONFIG, "$.array");
        configs.put(JoinFilterConfig.JOIN_SEPARATOR_CONFIG, "|");

        filter.configure(configs, alias -> null);
        final TypedStruct struct = TypedStruct.create().put("array", Arrays.asList("one", "two", "three", "four"));

        RecordsIterable<TypedStruct> filtered = filter.apply(context, struct, false);
        assertFalse(filtered.isEmpty());
        assertEquals(1, filtered.size());

        final TypedStruct joined = filtered.collect().get(0);

        assertEquals("one|two|three|four", joined.getString("array"));
    }

    @Test
    public void shouldJoinArrayGivenTarget() {
        configs.put(JoinFilterConfig.JOIN_FIELD_CONFIG, "$.array");
        configs.put(JoinFilterConfig.JOIN_TARGET_CONFIG, "$.joined");
        configs.put(JoinFilterConfig.JOIN_SEPARATOR_CONFIG, "|");

        filter.configure(configs, alias -> null);
        final TypedStruct struct = TypedStruct.create().put("array", Arrays.asList("one", "two", "three", "four"));

        RecordsIterable<TypedStruct> filtered = filter.apply(context, struct, false);
        assertFalse(filtered.isEmpty());
        assertEquals(1, filtered.size());

        final TypedStruct joined = filtered.collect().get(0);

        assertEquals("one|two|three|four", joined.getString("joined"));
    }

    @Test(expected = DataException.class)
    public void shouldThrowDataExceptionGivenNonArrayField() {
        configs.put(JoinFilterConfig.JOIN_FIELD_CONFIG, "array");
        filter.configure(configs, alias -> null);
        filter.apply(context, TypedStruct.create().put("array","dummy"), false);
    }

}