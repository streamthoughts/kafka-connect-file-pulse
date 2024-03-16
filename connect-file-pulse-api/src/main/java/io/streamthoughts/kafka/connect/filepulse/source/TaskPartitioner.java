/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public interface TaskPartitioner extends AutoCloseable {

    /**
     * Partitions the specified object-file URIs.
     *
     * @param files         the object-file URIs to partition.
     * @param taskCount     the total number of tasks.
     * @return              the list of URIs for the given {@literal taskId}.
     */
    List<List<URI>> partition(final Collection<FileObjectMeta> files, final int taskCount);

    /**
     * Partitions the specified object-file URIs.
     *
     * @param files         the object-file URIs to partition.
     * @param taskCount     the total number of tasks.
     * @param taskId        the task id.
     * @return              the list of URIs for the given {@literal taskId}.
     */
    default List<URI> partitionForTask(final Collection<FileObjectMeta> files,
                                       final int taskCount,
                                       final int taskId) {
        return files == null || files.isEmpty() ? Collections.emptyList() : partition(files, taskCount).get(taskId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    default void close() { }
}
