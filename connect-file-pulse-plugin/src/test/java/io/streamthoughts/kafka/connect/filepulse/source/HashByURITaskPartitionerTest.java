/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.Test;

public class HashByURITaskPartitionerTest {

    private final HashByURITaskPartitioner partitioner = new HashByURITaskPartitioner();

    @Test
    public void test() {
        List<FileObjectMeta> toPartition = new ArrayList<>(10);
        IntStream
            .range(0, 1)
            .forEachOrdered(i -> toPartition.add(new GenericFileObjectMeta(URI.create("file://tmp/" + i))));

        final List<List<URI>> partitioned = partitioner.partition(toPartition, 4);
        assertEquals(4, partitioned.size());
        System.out.println(partitioned);
    }

}