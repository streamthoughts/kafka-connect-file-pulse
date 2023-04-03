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