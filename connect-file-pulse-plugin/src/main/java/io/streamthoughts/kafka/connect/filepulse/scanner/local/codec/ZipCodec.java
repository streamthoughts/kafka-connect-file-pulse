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
package io.streamthoughts.kafka.connect.filepulse.scanner.local.codec;

import io.streamthoughts.kafka.connect.filepulse.internal.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipCodec implements CodecHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ZipCodec.class);

    private static final Set<String> MIME_TYPES = new HashSet<>();

    static  {
        MIME_TYPES.add("application/x-zip-compressed");
        MIME_TYPES.add("application/zip");
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
            return MIME_TYPES.contains(type);
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
        try (ZipInputStream inputStream = new ZipInputStream(new FileInputStream(file))) {
            ZipEntry zipEntry;
            String entryName, entryDir;
            while ((zipEntry = inputStream.getNextEntry()) != null) {
                entryName = zipEntry.getName();
                if (zipEntry.isDirectory()) {
                    continue;
                }

                entryDir = IOUtils.getParentDirectoryPath(entryName);
                if (entryDir != null) {
                    Files.createDirectories(Paths.get(parent.getAbsolutePath(), entryDir));
                }
                CodecHandlerUtils.decompress(inputStream, parent.getAbsolutePath(), entryName);
            }
        } catch (IOException e) {
            LOG.error("Error while extracting file {}", file.getName(), e);
        }
        return parent;
    }
}
