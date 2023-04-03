/*
 * Copyright 2019-2021 StreamThoughts.
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

import static io.streamthoughts.kafka.connect.filepulse.internal.IOUtils.createParentIfNotExists;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Storage} implementation for manging files on local file-system.
 */
public class LocalFileStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileStorage.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta getObjectMetadata(final URI uri) {
        return new LocalFileObjectMeta(new File(uri));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final URI uri) {
        return Files.exists(Paths.get(uri));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(final URI uri) {
        try {
            Files.deleteIfExists(Paths.get(uri));
            LOG.debug("Removed object file successfully: {}", uri);
        } catch (IOException e) {
            LOG.error("Failed to remove object file: {}", uri, e);
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean move(final URI source, final URI dest) {
        final Path sourcePath = Paths.get(source);
        final Path destPath = Paths.get(dest);
        try {
            LOG.info("Moving file '{}' to '{}'.", source, dest);
            createParentIfNotExists(destPath);
            Files.move(sourcePath, destPath, StandardCopyOption.ATOMIC_MOVE);
            LOG.info("File '{}' moved successfully", source);
        } catch (IOException outer) {
            try {
                Files.move(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
                LOG.debug(
                        "Non-atomic move of '{}' to '{}' succeeded after atomic move failed due to '{}'",
                        source,
                        destPath,
                        outer.getMessage()
                );
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                try {
                    doSimpleMove(sourcePath, destPath);
                    LOG.debug("Simple move as copy+delete of '{}' to '{}' succeeded after move failed due to '{}'",
                            source,
                            dest,
                            inner.getMessage()
                    );
                } catch (IOException e) {
                    e.addSuppressed(inner);
                    LOG.error("Error while moving file '{}'", source, inner);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputStream getInputStream(final URI uri) throws FileNotFoundException {
        return new FileInputStream(new File(uri));
    }

    /**
     * Simple move implements as copy+delete.
     *
     * @param source    the source path.
     * @param target    the target path.
     */
    static void doSimpleMove(final Path source, final Path target) throws IOException{
        // attributes of source file
        BasicFileAttributes attrs = Files.readAttributes(source, BasicFileAttributes.class);

        if (attrs.isSymbolicLink())
            throw new IOException("Copying of symbolic links not supported");

        // delete target if it exists
        Files.deleteIfExists(target);

        // copy file
        try (InputStream in = Files.newInputStream(source)) {
            Files.copy(in, target);
        }

        // try to copy basic attributes to target
        BasicFileAttributeView view = Files.getFileAttributeView(target, BasicFileAttributeView.class);
        try {
            view.setTimes(attrs.lastModifiedTime(), attrs.lastAccessTime(), attrs.creationTime());
        } catch (Throwable x) {
            LOG.debug("Failed to copy basic attributes while moving file {} to {}", source, target);
        }
        // finally delete source file
        Files.delete(source);
    }

}
