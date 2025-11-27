/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.fs.client.SmbClient;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link FileSystemListing} for SMB/CIFS file systems.
 */
public class SmbFileSystemListing implements FileSystemListing<SmbFileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(SmbFileSystemListing.class);

    private FileListFilter filter;
    private SmbFileSystemListingConfig config;
    private SmbClient smbClient;

    public SmbFileSystemListing(final List<FileListFilter> filters) {
        Objects.requireNonNull(filters, "filters can't be null");
        this.filter = new CompositeFileListFilter(filters);
    }

    @SuppressWarnings("unused")
    public SmbFileSystemListing() {
        this(Collections.emptyList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        LOG.debug("Configuring SmbFilesystemListing");
        config = new SmbFileSystemListingConfig(configs);
        smbClient = new SmbClient(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<FileObjectMeta> listObjects() {
        String listingDirectoryPath = getConfig().getSmbDirectoryPath();

        LOG.info("Listing SMB files in directory: smb://{}/{}{}",
                getConfig().getSmbHost(),
                getConfig().getSmbShare(),
                listingDirectoryPath);

        List<FileObjectMeta> filesMetadata = getSmbClient()
                .listFiles(listingDirectoryPath)
                .collect(Collectors.toList());

        LOG.info("Found {} files in SMB directory before filtering", filesMetadata.size());

        Collection<FileObjectMeta> filteredFiles = filter.filterFiles(filesMetadata);

        LOG.info("Returning {} files after filtering", filteredFiles.size());

        return filteredFiles;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFilter(FileListFilter filter) {
        this.filter = filter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SmbFileStorage storage() {
        return new SmbFileStorage(config);
    }

    SmbClient getSmbClient() {
        return smbClient;
    }

    SmbFileSystemListingConfig getConfig() {
        return config;
    }
}
