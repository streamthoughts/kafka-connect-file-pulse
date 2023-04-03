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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobProperties;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobStorage implements Storage {

    private static final String AZURE_BLOB_STORAGE_METADATA_PREFIX = "azure.blob.storage.";

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorage.class);

    private final BlobContainerClient containerClient;

    public AzureBlobStorage(final BlobContainerClient blobContainerClient) {
        this.containerClient = blobContainerClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta getObjectMetadata(final URI uri) {
        final BlobClient client = getBlobClient(uri);
        return getObjectMetadata(client);
    }

    GenericFileObjectMeta getObjectMetadata(final BlobClient client) {
        final String blobUrl = client.getBlobUrl();
        final URI uri;
        try {
            uri = new URI(blobUrl);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to build blob URI from '" + blobUrl + "'", e);
        }

        final HashMap<String, Object> userDefinedMetadata = new HashMap<>();
        userDefinedMetadata.put(withMetadataPrefix("account"), containerClient.getAccountName());
        userDefinedMetadata.put(withMetadataPrefix("blobUrl"), blobUrl);

        final BlobProperties properties = client.getProperties();

        Optional.ofNullable(properties.getCreationTime()).ifPresent(it ->
                userDefinedMetadata.put(withMetadataPrefix("creationTime"), it.toInstant().toEpochMilli()));
        Optional.ofNullable(properties.getContentEncoding()).ifPresent(it ->
                userDefinedMetadata.put(withMetadataPrefix("contentEncoding"), it));
        Optional.ofNullable(properties.getContentType()).ifPresent(it ->
                userDefinedMetadata.put(withMetadataPrefix("contentType"), it));

        properties.getMetadata()
                .entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .forEach(e -> userDefinedMetadata.put(withMetadataPrefix(e.getKey()), e.getValue()));

        return new GenericFileObjectMeta.Builder()
                .withUri(uri)
                .withName(client.getBlobName())
                .withContentLength(properties.getBlobSize())
                .withLastModified(properties.getLastModified().toInstant().toEpochMilli())
                .withContentDigest(null)
                .withUserDefinedMetadata(userDefinedMetadata)
                .build();
    }

    private static String withMetadataPrefix(final String account) {
        return AZURE_BLOB_STORAGE_METADATA_PREFIX + account;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final URI uri) {
        return getBlobClient(uri).exists();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(final URI uri) {
        final Response<Void> response = getBlobClient(uri).deleteWithResponse(
                null,
                null, null,
                Context.NONE
        );
        final boolean succeed = response.getStatusCode() == 200;
        if (!succeed) {
            LOG.debug(
                    "Failed to delete object file from AzureBlogStorage: "
                            + "uri={}, "
                            + "returned status code={}",
                    uri,
                    response.getStatusCode()
            );
        }
        return succeed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean move(final URI source, final URI dest) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream(final URI uri) {
        return getBlobClient(uri).openInputStream();
    }

    private BlobClient getBlobClient(final URI uri) {
        final String blobName = new BlobClientBuilder()
                .endpoint(uri.toString())
                .buildClient()
                .getBlobName();
        return containerClient.getBlobClient(blobName);
    }

    public BlobContainerClient getBlobContainerClient() {
        return containerClient;
    }
}
