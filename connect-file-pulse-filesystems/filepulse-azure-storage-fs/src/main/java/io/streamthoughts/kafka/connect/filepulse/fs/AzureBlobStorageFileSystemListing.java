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

import static io.streamthoughts.kafka.connect.filepulse.internal.StringUtils.isNotBlank;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AzureBlobStorageFileSystemListing} that can be used for listing objects
 * that exist in a specific Azure Blob Storage container.
 */
public class AzureBlobStorageFileSystemListing implements FileSystemListing<AzureBlobStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorageFileSystemListing.class);

    public static final Duration DEFAULT_TIME = null;

    private FileListFilter filter;
    private AzureBlobStorage storage;
    private AzureBlobStorageConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new AzureBlobStorageConfig(configs);
        this.storage = new AzureBlobStorage(AzureBlobStorageClientUtils.createBlobContainerClient(config));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<FileObjectMeta> listObjects() {
        LOG.debug("Listing objects in container '{}' using prefix '{}'",
                config.getContainerName(),
                config.getPrefix()
        );
        final BlobContainerClient blobContainerClient = storage.getBlobContainerClient();
        final ListBlobsOptions options = new ListBlobsOptions()
            .setDetails(new BlobListDetails()
                .setRetrieveMetadata(true)
            );

        if (isNotBlank(config.getPrefix())) {
            options.setPrefix(config.getPrefix());
        }

        final PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(options, DEFAULT_TIME);
        final List<FileObjectMeta> fileObjectMetaList = new LinkedList<>();
        for (final BlobItem item : blobItems) {
            LOG.debug("Find BlobItem with name '{}'", item.getName());
            final Boolean prefix = item.isPrefix();
            if (prefix != null && prefix) {
                LOG.info("Ignored virtual directory prefix: '{}'", item.getName());
            } else {
                final BlobClient blobClient = blobContainerClient.getBlobClient(item.getName());
                if (isDirectory(blobClient)) {
                    LOG.info("Ignored virtual directory prefix: '{}'", item.getName());
                } else {
                    final GenericFileObjectMeta objectMetadata = storage.getObjectMetadata(blobClient);
                    fileObjectMetaList.add(objectMetadata);
                }
            }
        }
        return filter == null ? fileObjectMetaList : filter.filterFiles(fileObjectMetaList);
    }

    private Boolean isDirectory(final BlobClient blobClient) {
        return Optional.ofNullable(blobClient.getProperties()
                .getMetadata().get("hdi_isfolder"))
                .map(Boolean::parseBoolean)
                .orElse(false);
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
    public AzureBlobStorage storage() {
        return storage;
    }
}
