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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.kafka.common.utils.Utils;

public class HashByURITaskPartitioner implements TaskPartitioner {

    /**
     * {@inheritDoc}
     */
    @Override
    public List<List<URI>> partition(final Collection<FileObjectMeta> files, final int taskCount) {

        if (files.isEmpty()) {
            return Collections.emptyList();
        }

        final ArrayList<List<URI>> partitioned = new ArrayList<>(taskCount);
        IntStream.range(0, taskCount).forEachOrdered(i -> partitioned.add(i, new LinkedList<>()));

        for (FileObjectMeta objectMeta : files) {
            final byte[] bytes = objectMeta.stringURI().getBytes(StandardCharsets.UTF_8);
            // TODO maybe we should not rely on Apache Kafka utility classes.
            final int taskId = Utils.toPositive(Utils.murmur2(bytes)) % taskCount;
            partitioned.get(taskId).add(objectMeta.uri());
        }

        return partitioned;
    }
}
