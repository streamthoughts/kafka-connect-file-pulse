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
package io.streamthoughts.kafka.connect.filepulse.fs.codec;

import io.streamthoughts.kafka.connect.filepulse.internal.IOUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TarballCodec implements CodecHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GZipCodec.class);

    private static final String TAR_GZ_SUFFIX = ".tar.gz";

    private static final Set<String> MIME_TYPES = new HashSet<>();

    static  {
        MIME_TYPES.add("application/x-tar");
    }

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
    public boolean canRead(final File file) {
        Objects.requireNonNull(file, "file can't be null");
        try {
            final String type = Files.probeContentType(file.toPath());
            return MIME_TYPES.contains(type) || file.getName().contains(TAR_GZ_SUFFIX);
        } catch (IOException e) {
            LOG.warn("Unexpected error occurred while proving content-type for file : {}", file.getAbsolutePath());
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File decompress(final File file) throws IOException {
        File parent = IOUtils.createDirectoryFromFile(file);
        try (TarArchiveInputStream inputStream = new TarArchiveInputStream(
                new FileInputStream(file))) {

            ArchiveEntry entry;
            String entryName;
            String entryDir;
            while ((entry = inputStream.getNextEntry()) != null) {

                entryName = entry.getName();
                if (entry.isDirectory()) {
                    continue;
                }

                entryDir = IOUtils.getParentDirectoryPath(entryName);
                if (entryDir != null) {
                    Path parentPath = Paths.get(parent.getAbsolutePath(), entryDir);
                    if (!Files.exists(parentPath)) {
                        Files.createDirectories(parentPath);
                    }
                }

                CodecHandlerUtils.decompress(inputStream, parent.getAbsolutePath(), entryName);
            }
        } catch (IOException e) {
            LOG.error("Error while extracting file {}", file.getName(), e);
        }

        return parent;
    }
}
