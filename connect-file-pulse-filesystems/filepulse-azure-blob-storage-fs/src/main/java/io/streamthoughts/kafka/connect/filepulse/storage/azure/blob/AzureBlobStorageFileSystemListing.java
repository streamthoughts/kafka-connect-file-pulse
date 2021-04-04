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
package io.streamthoughts.kafka.connect.filepulse.storage.azure.blob;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.streamthoughts.kafka.connect.filepulse.fs.FileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.storage.azure.AzureBlobStorageClientConfig;
import io.streamthoughts.kafka.connect.filepulse.storage.azure.AzureBlobStorageClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.streamthoughts.kafka.connect.filepulse.internal.StringUtils.isNotBlank;

public class AzureBlobStorageFileSystemListing implements FileSystemListing {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorageFileSystemListing.class);

    private FileListFilter filter;
    private AzureBlobStorage storage;
    private AzureBlobStorageClientConfig config;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new AzureBlobStorageClientConfig(configs);
        storage = new AzureBlobStorage(
                AzureBlobStorageClientUtils.createBlobContainerClient(config));
    }

    @Override
    public Collection<FileObjectMeta> listObjects() {
        BlobContainerClient blobContainerClient = storage.getBlobContainerClient();
        ListBlobsOptions options = new ListBlobsOptions();
        if (isNotBlank(config.getAzureBlobStoragePrefix()))
            options.setPrefix(config.getAzureBlobStoragePrefix());
        List<FileObjectMeta> fileObjectMetaList = blobContainerClient.listBlobs(options, null)
                .stream()
                .map(blobItem -> {
                    // URI construction, as the azure sdk sucks...
                    String blobUrl = blobContainerClient.getBlobClient(blobItem.getName()).getBlobUrl();
                    URI uri = null;
                    try {
                        uri = new URI(blobUrl);
                    } catch (URISyntaxException e) {
                        LOG.error(
                                "Failed to construct the blob URI from the given URL : '{}'. ",
                                blobUrl,
                                e
                        );
                    }
                    return storage.getObjectMetadata(blobItem, uri);
                }).collect(Collectors.toList());
        return filter == null ? fileObjectMetaList : filter.filterFiles(fileObjectMetaList);
    }

    @Override
    public void setFilter(FileListFilter filter) {
        this.filter = filter;
    }
}
