/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsFileSystemListing implements FileSystemListing<GcsStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystemListing.class);

    private GcsClientConfig config;

    private Storage gcsClient;

    private FileListFilter filter;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        config = new GcsClientConfig(configs);
        gcsClient = GcsClientUtils.createStorageService(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<FileObjectMeta> listObjects() {

        final String blobsPrefix = config.getBlobsPrefix();
        final String bucketName = config.getBucketName();

        final List<FileObjectMeta> objectMetaList = new LinkedList<>();
        try {
            final Page<Blob> blobs;
            if (blobsPrefix != null) {
                LOG.info(
                        "Listing the blobs in the bucket '{}' whose names begin with prefix '{}'",
                        bucketName,
                        blobsPrefix
                );
                blobs = gcsClient.list(bucketName, Storage.BlobListOption.prefix(blobsPrefix));
            } else {
                LOG.info(
                        "Listing the blobs in the bucket '{}'",
                        bucketName
                );
                blobs = gcsClient.list(bucketName);
            }
            for (Blob blob : blobs.iterateAll()) {
                if (isBlobMustBeIgnored(blob)) {
                    LOG.info("Ignored blob in bucket '{}' with name '{}' (is_directory={}, size={})",
                            blob.getBucket(),
                            blob.getName(),
                            blob.isDirectory(),
                            blob.getSize()
                    );
                } else{
                    objectMetaList.add(GcsStorage.createFileObjectMeta(blob));
                }
            }
        } catch (StorageException e) {
            LOG.error(
                    "Failed to list blobs from the Google Cloud Storage bucket '{}'. ",
                    config.getBucketName(),
                    e
            );
        }

        return filter == null ? objectMetaList : filter.filterFiles(objectMetaList);
    }

    private boolean isBlobMustBeIgnored(final Blob blob) {
        return blob.isDirectory()
                || blob.getName().endsWith("/")
                || blob.getSize() == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFilter(final FileListFilter filter) {
        this.filter = filter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GcsStorage storage() {
        return new GcsStorage(gcsClient);
    }
}
