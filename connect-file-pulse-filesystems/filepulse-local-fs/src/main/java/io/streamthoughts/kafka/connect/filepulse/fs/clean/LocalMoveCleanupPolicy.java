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
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import static io.streamthoughts.kafka.connect.filepulse.internal.IOUtils.isAbsolute;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.LocalFSDirectoryListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.internal.IOUtils;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Policy for moving atomically files to configurable target directories.
 */
public class LocalMoveCleanupPolicy implements FileCleanupPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMoveCleanupPolicy.class);

    private Storage storage;

    private MoveFileCleanerConfig configs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.configs = new MoveFileCleanerConfig(configs);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if no storage is set.
     */
    @Override
    public boolean onSuccess(final FileObject source) {
        checkState();
        final URI sourceURI = source.metadata().uri();

        if (!storage.exists(sourceURI)) {
            LOG.warn("Cannot move file '{}' to success path due to file does not exist.", sourceURI);
            return true;
        }
        final URI targetURI = buildTargetURI(configs.listingDirectoryPath(), sourceURI, configs.outputSucceedPath());
        return storage.move(sourceURI, targetURI);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if no storage is set.
     */
    @Override
    public boolean onFailure(final FileObject source) {
        checkState();
        final URI sourceURI = source.metadata().uri();
        if (!storage.exists(sourceURI)) {
            LOG.warn("Cannot move file '{}' to error path due to file does not exist.", sourceURI);
            return true;
        }
        final URI targetURI = buildTargetURI(configs.listingDirectoryPath(), sourceURI, configs.outputFailedPath());
        return storage.move(sourceURI, targetURI);
    }

    private static URI buildTargetURI(final String scannedDirectory,
                                      final URI sourceURI,
                                      final String target) {
        final File source = new File(sourceURI);
        if (isAbsolute(target)) {
            final String relative = IOUtils.getRelativePathFrom(scannedDirectory, source);
            return Paths.get(target, relative, source.getName()).toUri();
        } else {
            final String parent = source.getParentFile().getAbsolutePath();
            return Paths.get(parent, target, source.getName()).toUri();
        }
    }

    private void checkState() {
        if (storage == null) {
            throw new IllegalStateException("no 'storage' initialized.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {
        this.storage = storage;
    }

    public static class MoveFileCleanerConfig extends AbstractConfig {

        final static String CLEANER_OUTPUT_FAILED_PATH_CONFIG = "cleaner.output.failed.path";
        final static String CLEANER_OUTPUT_FAILED_PATH_DOC =
                "Target directory for file proceed with failure (default .failure)";

        final static String CLEANER_OUTPUT_SUCCEED_PATH_CONFIG = "cleaner.output.succeed.path";
        final static String CLEANER_OUTPUT_SUCCEED_PATH_DOC =
                "Target directory for file proceed successfully (default .success)";

        MoveFileCleanerConfig(final Map<?, ?> originals) {
            super(getConf(), originals);
        }

        String outputFailedPath() {
            return getString(CLEANER_OUTPUT_FAILED_PATH_CONFIG);
        }

        String outputSucceedPath() {
            return getString(CLEANER_OUTPUT_SUCCEED_PATH_CONFIG);
        }

        String listingDirectoryPath() {
            return getString(LocalFSDirectoryListingConfig.FS_LISTING_DIRECTORY_PATH);
        }

        static ConfigDef getConf() {
            return new ConfigDef()
                    .define(CLEANER_OUTPUT_FAILED_PATH_CONFIG, ConfigDef.Type.STRING, ".failure",
                            ConfigDef.Importance.HIGH, CLEANER_OUTPUT_FAILED_PATH_DOC)

                    .define(CLEANER_OUTPUT_SUCCEED_PATH_CONFIG, ConfigDef.Type.STRING, ".success",
                            ConfigDef.Importance.HIGH, CLEANER_OUTPUT_SUCCEED_PATH_DOC)

                    .define(LocalFSDirectoryListingConfig.FS_LISTING_DIRECTORY_PATH, ConfigDef.Type.STRING,
                            ConfigDef.Importance.HIGH, LocalFSDirectoryListingConfig.FS_LISTING_DIRECTORY_DOC);
        }
    }
}