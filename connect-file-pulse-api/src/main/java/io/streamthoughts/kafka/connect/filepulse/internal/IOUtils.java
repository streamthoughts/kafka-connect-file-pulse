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
package io.streamthoughts.kafka.connect.filepulse.internal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

/**
 * Utility methods for manipulating I/O files.
 */
public class IOUtils {

    public static Optional<Long> getUnixInode(final File file) {
        Objects.requireNonNull(file, "file can't be null");
        try {
            final Long inode =  (Long) Files.getAttribute(file.toPath(), "unix:ino");
            return Optional.ofNullable(inode);
        }
        catch (IOException | UnsupportedOperationException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static void createParentIfNotExists(final Path targetPath) throws IOException {
        Objects.requireNonNull(targetPath, "'targetPath' cannot be null");
        Path parent = targetPath.getParent();
        if (!Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }

    public static boolean isAbsolute(final String to) {
        return to.startsWith(File.separator) || new File(to).isAbsolute();
    }

    public static String getRelativePathFrom(final String basePath, final File file) {
        Objects.requireNonNull(basePath, "basePath cannot be null");
        Objects.requireNonNull(file, "file cannot be null");
        return new File(basePath)
                .toURI()
                .relativize(file.getParentFile().toURI()).getPath();
    }

    public static String getParentDirectoryPath(final String name) {
        int sepIndex = -1;
        if (name.contains(File.separator)) {
            sepIndex = name.lastIndexOf(File.separator);
        }
        return (sepIndex == -1) ? null : name.substring(0, sepIndex);
    }

    public static String getNameWithoutExtension(final File file) {
        Objects.requireNonNull(file, "file cannot be null");
        String filename = file.getName();
        int dotIndex = filename.lastIndexOf(".");
        return (dotIndex == -1) ? filename : filename.substring(0, dotIndex);
    }

    public static File createDirectoryFromFile(final File file) throws IOException {
        Objects.requireNonNull(file, "file cannot be null");
        final Path unzipPath = Paths.get(file.getParentFile().getCanonicalPath(), getNameWithoutExtension(file));
        if (!Files.exists(unzipPath)) {
            Files.createDirectories(unzipPath);
        }
        return unzipPath.toFile();
    }
}