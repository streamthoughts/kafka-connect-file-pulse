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
