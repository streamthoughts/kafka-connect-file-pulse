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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;

import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;

public class AzureBlobStorage implements Storage {

    private final BlobContainerClient containerClient;

    public AzureBlobStorage(BlobContainerClient blobContainerClient) {
        this.containerClient = blobContainerClient;
    }

    @Override
    public FileObjectMeta getObjectMetadata(URI uri) {
        BlobClient blobClient = getBlobClient(uri);

        return new GenericFileObjectMeta.Builder()
                .withUri(uri)
                .withName(blobClient.getBlobName())
                .withContentLength(blobClient.getProperties().getBlobSize())
                .withLastModified(blobClient.getProperties().getLastModified().toInstant())
                .withContentDigest(
                        new FileObjectMeta.ContentDigest(
                                Arrays.toString(blobClient.getProperties().getContentMd5()), "MD5"))
                .withUserDefinedMetadata(new HashMap<>(blobClient.getProperties().getMetadata()))
                .build();
    }

    public FileObjectMeta getObjectMetadata(BlobItem blobItem, URI uri) {
        return new GenericFileObjectMeta.Builder()
                .withUri(uri)
                .withName(blobItem.getName())
                .withContentLength(blobItem.getProperties().getContentLength())
                .withLastModified(blobItem.getProperties().getLastModified().toInstant())
                .withContentDigest(
                        new FileObjectMeta.ContentDigest(
                                Arrays.toString(blobItem.getProperties().getContentMd5()), "MD5"))
                .withUserDefinedMetadata(new HashMap<>(blobItem.getMetadata()))
                .build();
    }

    @Override
    public boolean exists(URI uri) {
        return getBlobClient(uri).exists();
    }

    @Override
    public InputStream getInputStream(URI uri) {
        return getBlobClient(uri).openInputStream();
    }

    private BlobClient getBlobClient(URI uri) {
        return containerClient
                .getBlobClient(new BlobClientBuilder().endpoint(uri.toString()).buildClient().getBlobName());
    }

    public BlobContainerClient getBlobContainerClient() {
        return containerClient;
    }
}
