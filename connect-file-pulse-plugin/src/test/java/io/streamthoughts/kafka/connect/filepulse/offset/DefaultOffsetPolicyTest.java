/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.offset;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class DefaultOffsetPolicyTest {

    private static final GenericFileObjectMeta metadata = new GenericFileObjectMeta(
            URI.create("file:///tmp/path/test"),
            "test",
            0L,
            123L,
            new FileObjectMeta.ContentDigest("789", "dummy"),
            Collections.singletonMap(LocalFileObjectMeta.SYSTEM_FILE_INODE_META_KEY, "456L")
    );

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_illegal_argument_given_empty_strategy() {
        new DefaultSourceOffsetPolicy("").toPartitionMap(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_illegal_argument_given_unknown_strategy() {
        new DefaultSourceOffsetPolicy("dummy").toPartitionMap(metadata);
    }

    @Test(expected = NullPointerException.class)
    public void should_throw_npe_given_unknown_strategy() {
        new DefaultSourceOffsetPolicy(null).toPartitionMap(metadata);
    }

    @Test
    public void should_get_offset_based_on_path() {
        Map<String, Object> result = new DefaultSourceOffsetPolicy("PATH").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("/tmp/path", result.get("path"));
    }

    @Test
    public void should_get_offset_based_on_hash() {
        Map<String, Object> result = new DefaultSourceOffsetPolicy("HASH").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("789", result.get("hash"));
    }

    @Test
    public void should_get_offset_based_on_modified() {
        Map<String, Object> result = new DefaultSourceOffsetPolicy("LASTMODIFIED").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(123L, result.get("lastmodified"));

    }

    @Test
    public void should_get_offset_based_on_name() {
        Map<String, Object> result = new DefaultSourceOffsetPolicy("NAME").toPartitionMap(metadata);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("test", result.get("name"));

    }

    @Test
    public void should_get_composed_offset_based_on_path_and_hash() {
        Map<String, Object> result = new DefaultSourceOffsetPolicy("PATH+HASH").toPartitionMap(metadata);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("/tmp/path", result.get("path"));
        Assert.assertEquals("789", result.get("hash"));
    }

    @Test
    public void should_support_strategies_in_any_order() {
        final String o1 = new DefaultSourceOffsetPolicy("PATH+HASH").toPartitionMap(metadata)
                .entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));

        final String o2 = new DefaultSourceOffsetPolicy("HASH+PATH").toPartitionMap(metadata)
                .entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));

        Assert.assertEquals(o1, o2);
    }
}