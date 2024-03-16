/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.fs.AzureBlobStorage;
import io.streamthoughts.kafka.connect.filepulse.fs.AzureBlobStorageClientUtils;
import io.streamthoughts.kafka.connect.filepulse.fs.AzureBlobStorageConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.AzureBlobStorageFileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AzureBlobStorageInputReader extends AbstractFileInputReader
        implements StorageAwareFileInputReader<AzureBlobStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorageFileSystemListing.class);

    protected AzureBlobStorage storage;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);

        if (storage == null) {
            LOG.info("Create new AZ Storage Client from the properties passed through the connector's configuration");
            storage = new AzureBlobStorage(
                    AzureBlobStorageClientUtils.createBlobContainerClient(
                            new AzureBlobStorageConfig(configs)));
        }
    }

    @Override
    public AzureBlobStorage storage() {
        return storage;
    }
}
