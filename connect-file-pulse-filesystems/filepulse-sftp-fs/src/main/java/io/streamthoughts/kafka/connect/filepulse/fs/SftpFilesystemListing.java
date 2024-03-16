/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.fs.client.SftpClient;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpFilesystemListing implements FileSystemListing<SftpFileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(SftpFilesystemListing.class);
    private FileListFilter filter;

    private SftpFilesystemListingConfig config;

    private SftpClient sftpClient;

    public SftpFilesystemListing(final List<FileListFilter> filters) {
        Objects.requireNonNull(filters, "filters can't be null");
        this.filter = new CompositeFileListFilter(filters);
    }

    @SuppressWarnings("unused")
    public SftpFilesystemListing() {
        this(Collections.emptyList());
    }
    /** {@inheritDoc} **/
    @Override
    public void configure(final Map<String, ?> configs) {
        LOG.debug("Configuring SftpFilesystemListing");
        config = new SftpFilesystemListingConfig(configs);
        sftpClient = new SftpClient(config);
    }

    /** {@inheritDoc} **/
    @Override
    public Collection<FileObjectMeta> listObjects() {
        String listingDirectoryPath = getConfig().getSftpListingDirectoryPath();

        List<FileObjectMeta> filesMetadata = getSftpClient()
                .listFiles(listingDirectoryPath)
                .collect(Collectors.toList());

        return filter.filterFiles(filesMetadata);
    }

    /** {@inheritDoc} **/
    @Override
    public void setFilter(FileListFilter filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} **/
    @Override
    public SftpFileStorage storage() {
        return new SftpFileStorage(config);
    }

    SftpClient getSftpClient() {
        return sftpClient;
    }

    SftpFilesystemListingConfig getConfig() {
        return config;
    }
}
