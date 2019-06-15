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
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.offset.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.offset.SourceOffset;
import org.apache.kafka.common.Configurable;

import java.util.Map;

/**
 * Default interface to define the policies to apply after a file has been proceed with success of failure.
 */
public interface FileCleanupPolicy extends AutoCloseable, Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    void configure(final Map<String, ?> configs);

    /**
     * Execute the cleanup policy for the specified input files in success.
     *
     * @param relativePath  the file path relative to the scanned directory.
     * @param metadata      the {@link SourceMetadata} of the file to be cleaned.
     * @param offset        the {@link SourceOffset} of the file to be cleaned.
     * @return {@code true} if the specified {@literal file} has been cleanup successfully.
     */
    boolean cleanOnSuccess(final String relativePath,
                           final SourceMetadata metadata,
                           final SourceOffset offset);

    /**
     * Execute the cleanup policy for the specified input files in failure.
     *
     * @param relativePath  the file path relative to the scanned directory.
     * @param metadata      the {@link SourceMetadata} of the file to be cleaned.
     * @param offset        the {@link SourceOffset} of the file to be cleaned.
     * @return {@code true} if the specified {@literal file} has been cleanup successfully.
     */
    boolean cleanOnFailure(final String relativePath,
                           final SourceMetadata metadata,
                           final SourceOffset offset);

    /**
     * Close any internal resources.
     */
    @Override
    void close() throws Exception;
}