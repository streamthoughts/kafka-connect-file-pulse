/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
