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
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

/**
 * Policy for deleting completed files.
 */
public class DeleteCleanupPolicy implements FileCleanupPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteCleanupPolicy.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onSuccess(final SourceFile source) {
        final SourceMetadata metadata = source.metadata();
        return doCleanup(new File(metadata.absolutePath()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final SourceFile source) {
        final SourceMetadata metadata = source.metadata();
        return doCleanup(new File(metadata.absolutePath()));
    }

    private boolean doCleanup(final File file) {
        try {
            Files.deleteIfExists(file.toPath());
            LOG.debug("Finished to cleanup input file successfully : {}", file.getAbsolutePath());
        } catch (IOException e) {
            LOG.error("Error while removing file {}", file.getAbsolutePath(), e);
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {

    }
}