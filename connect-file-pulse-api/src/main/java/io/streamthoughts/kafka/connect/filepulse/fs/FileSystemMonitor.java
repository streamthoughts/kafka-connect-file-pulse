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
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.List;
import org.apache.kafka.connect.connector.ConnectorContext;

/**
 * A {@code FileSystemMonitor} is responsible for monitoring a specific file-system
 * for new files to stream into Kafka.
 */
public interface FileSystemMonitor {

    /**
     * Run a single filesystem scan using the specified context. This method must invoke
     * the {@link ConnectorContext#requestTaskReconfiguration()} when new object-files can be scheduled.
     *
     * @param context   the connector context.
     */
    void invoke(final ConnectorContext context);

    /**
     * Enables or disables the file-listing process either temporarily or permanently.
     * In other words, if disabled then {@link #listFilesToSchedule()} will always return an empty list.
     *
     * @param enabled is the file-listing process enabled.
     */
    void setFileSystemListingEnabled(final boolean enabled);

    /**
     * Retrieves the list of objects-files that were found during the last the {@link #invoke(ConnectorContext)} call.
     * This method should not return more than the given maximum.
     *
     * @return                             the list of {@link FileObjectMeta} to schedule.
     */
    default List<FileObjectMeta> listFilesToSchedule() {
        return listFilesToSchedule(Integer.MAX_VALUE);
    }

    /**
     * Retrieves the list of objects-files that were found during the last the {@link #invoke(ConnectorContext)} call.
     * This method should not return more than the given maximum.
     *
     * @param           maxFilesToSchedule the maximum number of files that can be schedules to tasks.
     * @return                             the list of {@link FileObjectMeta} to schedule.
     */
    List<FileObjectMeta> listFilesToSchedule(final int maxFilesToSchedule);

    /**
     * Close underlying I/O resources.
     */
    void close();
}
