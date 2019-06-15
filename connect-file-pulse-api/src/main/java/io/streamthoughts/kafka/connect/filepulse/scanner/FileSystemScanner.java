/*
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
package io.streamthoughts.kafka.connect.filepulse.scanner;

import org.apache.kafka.connect.connector.ConnectorContext;

import java.util.List;

/**
 * A {@link FileSystemScanner} is responsible to scan a specific file system
 * for new files to stream into Kafka;
 */
public interface FileSystemScanner {

    /**
     * Run a single file system scan using the specified context.
     * @param context   the connector context.
     */
    void scan(final ConnectorContext context);

    /**
     * Gets newest files found during last scan partitioned for the specified number of groups.
     *
     * @param maxGroups the maximum number of groups.
     * @return          list of files to execute.
     */
    List<List<String>> partitionFilesAndGet(final int maxGroups);

    /**
     * Close underlying I/O resources.
     */
    void close();
}
