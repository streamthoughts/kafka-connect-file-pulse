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

import io.streamthoughts.kafka.connect.filepulse.fs.codec.CodecHandler;
import io.streamthoughts.kafka.connect.filepulse.fs.codec.CodecManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.LocalFileStorage;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The {@code LocalFSDirectoryListing} can be used for listing files that exist in an local input directory.
 */
public class LocalFSDirectoryListing implements FileSystemListing<LocalFileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFSDirectoryListing.class);

    private FileListFilter filter;

    private final CodecManager codecs;

    private LocalFSDirectoryListingConfig config;

    /**
     * Creates a new {@link LocalFSDirectoryListing} instance.
     */
    public LocalFSDirectoryListing() {
        this(Collections.emptyList());
    }

    /**
     * Creates a new {@link LocalFSDirectoryListing} instance.
     *
     * @param filters  the list of filters
     */
    public LocalFSDirectoryListing(final List<FileListFilter> filters) {
        Objects.requireNonNull(filters, "filters can't be null");
        this.filter = new CompositeFileListFilter(filters);
        this.codecs = new CodecManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        config = new LocalFSDirectoryListingConfig(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<FileObjectMeta> listObjects() throws IllegalArgumentException {
        List<File> files = listEligibleFiles(new File(config.listingDirectoryPath()));
        return this.filter != null ? this.filter.filterFiles(toSourceObjects(files)) : toSourceObjects(files);
    }

    private Collection<FileObjectMeta> toSourceObjects(final Collection<File> allFiles) {
        return allFiles.stream()
            .map(LocalFileObjectMeta::new)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFilter(final FileListFilter filter) {
        this.filter = filter;
    }

    private List<File> listEligibleFiles(final File input) {
        final List<File> listingLocalFiles = new LinkedList<>();
        if (!isReadableAndNotHidden(input)) {
            if (!input.isHidden()) {
                LOG.warn("File doesn't exist or can't be read: {}", input.getAbsolutePath());
            }
            return listingLocalFiles;
        }
        final List<File> decompressedDirs = new LinkedList<>();
        final List<File> directories = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(input.toPath())) {
            stream.forEach(path -> {
                final File file = path.toFile();
                try {
                    // directory path is already listed by the parent listEligibleFiles method.
                    if (file.isFile()) {
                        final CodecHandler codec = codecs.getCodecIfCompressedOrNull(file);
                        if (codec != null) {
                            LOG.debug("Detecting compressed file : {}", file.getCanonicalPath());
                            final File decompressed = codec.decompress(file);
                            listingLocalFiles.addAll(listEligibleFiles(decompressed));
                            decompressedDirs.add(decompressed);
                        } else {
                            // If no codec is found for the input file -
                            // we just naively consider it to be an uncompressed.
                            listingLocalFiles.add(file);
                        }
                    } else {
                        // A directory can be the result of a decompressed file.
                        // Defer scan after all compress files has been proceed.
                        directories.add(file);
                    }
                } catch (IOException e) {
                    LOG.error("Skip input file {} - error while decompressing", file.getName(), e);
                }
            });
        } catch (IOException e) {
            LOG.warn("Error while listing directory {}: {}", input.getAbsolutePath(), e.getLocalizedMessage());
            throw new ConnectException(e);
        }

        if (config.isRecursiveScanEnable()) {
            listingLocalFiles.addAll(directories.stream()
                .filter(f -> !decompressedDirs.contains(f))
                .flatMap(f -> listEligibleFiles(f).stream())
                .collect(Collectors.toList()));
        }
        return listingLocalFiles;
    }

    private boolean isReadableAndNotHidden(final File file) {
        return file.exists() && file.canRead() && !file.isHidden();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[directory.path=" + config.listingDirectoryPath() + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalFileStorage storage() {
        return new LocalFileStorage();
    }
}