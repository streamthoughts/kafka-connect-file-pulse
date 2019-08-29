/*
 * Copyright 2019 StreamThoughts.
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

import static io.streamthoughts.kafka.connect.filepulse.internal.IOUtils.createParentIfNotExists;
import static io.streamthoughts.kafka.connect.filepulse.internal.IOUtils.isAbsolute;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import io.streamthoughts.kafka.connect.filepulse.config.ConnectorConfig;
import io.streamthoughts.kafka.connect.filepulse.internal.IOUtils;
import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Policy for moving atomically files to configurable target directories.
 */
public class MoveCleanupPolicy implements FileCleanupPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(MoveCleanupPolicy.class);

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
     */
    @Override
    public boolean onSuccess(final SourceFile source) {
        final File file = source.file();
        return doCleanup(file, buildTargetPath(configs.scanDirectoryPath(), file, configs.outputSucceedPath()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final SourceFile source) {
        final File file = source.file();
        return doCleanup(file, buildTargetPath(configs.scanDirectoryPath(), file, configs.outputFailedPath()));
    }

    private static Path buildTargetPath(final String scannedDirectory,
                                        final File source,
                                        final String target) {
        final String name = source.getName();

        if (isAbsolute(target)) {
            final String relative = IOUtils.getRelativePathFrom(scannedDirectory, source);
            return Paths.get(target, relative, name);
        } else {
            final String parent = source.getParentFile().getAbsolutePath();
            return Paths.get(parent, target, name);
        }
    }

    /**
     * Attempts to atomically the source file to the specified target directory.
     *
     * @param source    the file to be moved.
     * @param target    the target directory.
     * @return  {@code true} if the file has been moved successfully.
     */
    private boolean doCleanup(final File source, final Path target) {
        final Path sourcePath = source.toPath();
        try {
            LOG.info("Moving file {} to {}", source, target.toFile());
            createParentIfNotExists(target);
            Files.move(sourcePath, target, StandardCopyOption.ATOMIC_MOVE);
            LOG.info("File {} moved successfully", source);
        } catch (IOException outer) {
            try {
                Files.move(sourcePath, target, StandardCopyOption.REPLACE_EXISTING);
                LOG.debug(
                    "Non-atomic move of {} to {} succeeded after atomic move failed due to {}",
                    source,
                        target,
                    outer.getMessage());
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                LOG.error("Error while moving file {}", source, inner);
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {

    }

    public static class MoveFileCleanerConfig extends AbstractConfig {

        final static String CLEANER_OUTPUT_FAILED_PATH_CONFIG   = "cleaner.output.failed.path";
        final static String CLEANER_OUTPUT_FAILED_PATH_DOC      =
                "Target directory for file proceed with failure (default .failure)";

        final static String CLEANER_OUTPUT_SUCCEED_PATH_CONFIG  = "cleaner.output.succeed.path";
        final static String CLEANER_OUTPUT_SUCCEED_PATH_DOC     =
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

        String scanDirectoryPath() { return getString(ConnectorConfig.FS_SCAN_DIRECTORY_PATH_CONFIG); }

        static ConfigDef getConf() {
            return new ConfigDef()
                    .define(CLEANER_OUTPUT_FAILED_PATH_CONFIG, ConfigDef.Type.STRING, ".failure",
                            ConfigDef.Importance.HIGH, CLEANER_OUTPUT_FAILED_PATH_DOC)

                    .define(CLEANER_OUTPUT_SUCCEED_PATH_CONFIG, ConfigDef.Type.STRING, ".success",
                            ConfigDef.Importance.HIGH, CLEANER_OUTPUT_SUCCEED_PATH_DOC)

                    .define(ConnectorConfig.FS_SCAN_DIRECTORY_PATH_CONFIG, ConfigDef.Type.STRING,
                            ConfigDef.Importance.HIGH, ConnectorConfig.FS_SCAN_DIRECTORY_PATH_DOC);
        }
    }
}