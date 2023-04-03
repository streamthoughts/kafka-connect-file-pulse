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

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.codec.CodecHandler;
import io.streamthoughts.kafka.connect.filepulse.fs.codec.CodecManager;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
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
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code LocalFSDirectoryListing} can be used for listing files that exist in a local input directory.
 */
public class LocalFSDirectoryListing implements FileSystemListing<LocalFileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFSDirectoryListing.class);

    private FileListFilter filter;

    private final CodecManager codecs;

    private LocalFSDirectoryListingConfig config;

    /**
     * Creates a new {@link LocalFSDirectoryListing} instance.
     * This no-arg constructor is required for the connector.
     */
    public LocalFSDirectoryListing() {
        this(Collections.emptyList());
    }


    /**
     * Creates a new {@link LocalFSDirectoryListing} instance.
     *
     * @param filters the list of filters
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
        List<File> files = listEligibleFiles(Path.of(config.listingDirectoryPath()));
        return this.filter != null ? this.filter.filterFiles(toSourceObjects(files)) : toSourceObjects(files);
    }

    private Collection<FileObjectMeta> toSourceObjects(final Collection<File> allFiles) {
        return allFiles.stream()
                .map(f -> {
                    try {
                        return Optional.of(new LocalFileObjectMeta(f));
                    } catch (ConnectFilePulseException e) {
                        LOG.warn(
                                "Failed to read metadata. Object file is ignored: {}",
                                e.getMessage()
                        );
                        return Optional.<LocalFileObjectMeta>empty();
                    }
                })
                .flatMap(Optional::stream)
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFilter(final FileListFilter filter) {
        this.filter = filter;
    }

    private List<File> listEligibleFiles(final Path input) {
        final List<File> listingLocalFiles = new LinkedList<>();
        if (!Files.isReadable(input)) {
            LOG.warn("Cannot get directory listing for '{}'. Input path is not readable.", input);
            return listingLocalFiles;
        }

        if (!Files.isDirectory(input)) {
            LOG.warn("Cannot get directory listing for '{}'. Input path is not a directory.", input);
            return listingLocalFiles;
        }

        if (isHidden(input)) {
            return listingLocalFiles;
        }

        final List<Path> decompressedDirs = new LinkedList<>();
        final List<Path> directories = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(input)) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    // A directory can be the result of a decompressed file.
                    // Defer scan after all compressed files has been proceeded.
                    directories.add(path);
                    continue;
                }

                if (Files.isReadable(path)) {
                    final File file = path.toFile();
                    final CodecHandler codec = codecs.getCodecIfCompressedOrNull(file);
                    if (codec != null) {
                        LOG.debug("Detecting compressed file : {}", file.getCanonicalPath());
                        try {
                            final Path decompressed = codec.decompress(file).toPath();
                            listingLocalFiles.addAll(listEligibleFiles(decompressed));
                            decompressedDirs.add(decompressed);
                        } catch (IOException e) {
                            LOG.warn("Error while decompressing input file '{}'. Skip and continue.", path, e);
                        }
                    } else {
                        // If no codec was found for the input file,
                        // then we just naively consider it to be uncompressed.
                        listingLocalFiles.add(file);
                    }
                } else {
                    LOG.warn("Input file is not readable '{}'. Skip and continue.", path);
                }
            }
        } catch (IOException e) {
            LOG.error(
                    "Error while getting directory listing for {}: {}",
                    input,
                    e.getLocalizedMessage()
            );
            throw new ConnectException(e);
        }

        if (config.isRecursiveScanEnable() && !directories.isEmpty()) {
            listingLocalFiles.addAll(directories.stream()
                    .filter(f -> !decompressedDirs.contains(f))
                    .flatMap(f -> listEligibleFiles(f).stream())
                    .collect(Collectors.toList())
            );
        }
        return listingLocalFiles;
    }

    private boolean isHidden(final Path input) {
        try {
            return Files.isHidden(input);
        } catch (IOException e) {
            LOG.warn(
                "Error while checking if input file is hidden '{}': {}",
                input,
                e.getLocalizedMessage()
            );
            return false;
        }
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