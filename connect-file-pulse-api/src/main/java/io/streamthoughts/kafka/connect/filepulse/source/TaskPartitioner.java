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
