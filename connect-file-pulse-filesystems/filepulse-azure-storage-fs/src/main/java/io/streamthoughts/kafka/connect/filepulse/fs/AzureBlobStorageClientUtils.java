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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating new {@link BlobContainerClient} client.
 */
public final class AzureBlobStorageClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorageClientUtils.class);

    public static BlobContainerClient createBlobContainerClient(final AzureBlobStorageConfig config) {
        Objects.requireNonNull(config, "config should not be null");

        final Optional<Password> optionalConnectionString = config.getConnectionString();
        if (optionalConnectionString.isPresent()) {
            LOG.info("Creating new BlobContainerClient using the connection.string that was passed "
                    + "through the connector's configuration");
            final String connectionString = optionalConnectionString.get().value();
            return new BlobServiceClientBuilder()
                    .connectionString(connectionString).buildClient()
                    .getBlobContainerClient(config.getContainerName());
        }

        final Optional<String> optionalAccountName = config.getAccountName();
        final Optional<Password> optionalAccountKey = config.getAccountKey();
        if (optionalAccountName.isPresent() && optionalAccountKey.isPresent()) {
            LOG.info("Creating new BlobContainerClient using the account name and "
                    + "the account key that were passed "
                    + "through the connector's configuration");

            final String accountName = optionalAccountName.get();
            final String accountKey = optionalAccountKey.get().value();

            return getBlobContainerClient(accountName, accountKey, config.getContainerName());
        }

        LOG.info("Cannot create BlobContainerClient using the connector's configuration. "
                + "No connection string or account name and account key were passed. "
                + "Trying to initialize client using environment variables.");

        final String accountName = System.getenv("AZURE_STORAGE_ACCOUNT");
        final String accountKey = System.getenv("AZURE_STORAGE_KEY");

        if (accountName != null && accountKey != null) {
            LOG.info("Creating new BlobContainerClient using the AZURE_STORAGE_ACCOUNT and "
                    + "the AZURE_STORAGE_KEY environment variables");
            return getBlobContainerClient(accountName, accountKey, config.getContainerName());
        }

        throw new ConfigException("Failed to create a new BlobContainerClient");
    }

    private static BlobContainerClient getBlobContainerClient(final String accountName,
                                                              final String accountKey,
                                                              final String containerName) {
        final StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(
                accountName,
                accountKey
        );

        return new BlobServiceClientBuilder()
                .endpoint(getEndpoint(accountName))
                .credential(sharedKeyCredential)
                .buildClient()
                .getBlobContainerClient(containerName);
    }


    private static String getEndpoint(final String accountName) {
        return String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
    }
}
