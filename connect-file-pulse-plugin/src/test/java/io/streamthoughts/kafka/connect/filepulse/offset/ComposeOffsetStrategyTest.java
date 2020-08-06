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

package io.streamthoughts.kafka.connect.filepulse.offset;

import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;

public class ComposeOffsetStrategyTest {

    private static final SourceMetadata metadata = new SourceMetadata(
            "test",
            "/tmp/path",
            0L,
            123L,
            456L,
            789L
    );

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_illegal_argument_given_empty_strategy() {
        new ComposeOffsetStrategy("").toPartitionMap(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_illegal_argument_given_unknown_strategy() {
        new ComposeOffsetStrategy("dummy").toPartitionMap(metadata);
    }

    @Test(expected = NullPointerException.class)
    public void should_throw_npe_given_unknown_strategy() {
        new ComposeOffsetStrategy(null).toPartitionMap(metadata);
    }

    @Test
    public void should_get_offset_based_on_path() {
        Map<String, Object> result = new ComposeOffsetStrategy("PATH").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("/tmp/path", result.get("path"));
    }

    @Test
    public void should_get_offset_based_on_hash() {
        Map<String, Object> result = new ComposeOffsetStrategy("HASH").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(789L, result.get("hash"));
    }

    @Test
    public void should_get_offset_based_on_modified() {
        Map<String, Object> result = new ComposeOffsetStrategy("LASTMODIFIED").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(123L, result.get("lastmodified"));

    }

    @Test
    public void should_get_offset_based_on_name() {
        Map<String, Object> result = new ComposeOffsetStrategy("NAME").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("test", result.get("name"));

    }

    @Test
    public void should_get_composed_offset_based_on_path_and_hash() {
        Map<String, Object> result = new ComposeOffsetStrategy("PATH+HASH").toPartitionMap(metadata);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("/tmp/path", result.get("path"));
        Assert.assertEquals(789L, result.get("hash"));
    }

    @Test
    public void should_support_strategies_in_any_order() {
        final String o1 = new ComposeOffsetStrategy("PATH+HASH").toPartitionMap(metadata)
                .entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));

        final String o2 = new ComposeOffsetStrategy("HASH+PATH").toPartitionMap(metadata)
                .entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));

        Assert.assertEquals(o1, o2);
    }
}