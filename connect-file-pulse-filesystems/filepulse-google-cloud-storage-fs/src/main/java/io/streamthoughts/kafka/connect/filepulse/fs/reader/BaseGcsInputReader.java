/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import com.google.cloud.storage.Storage;
import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.fs.GcsClientConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.GcsClientUtils;
import io.streamthoughts.kafka.connect.filepulse.fs.GcsStorage;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import java.util.Map;

/**
 * The {@code BaseGcsInputReader} provides the {@link GcsStorage}.
 */
public abstract class BaseGcsInputReader extends AbstractFileInputReader
        implements StorageAwareFileInputReader<GcsStorage> {

    private GcsStorage storage;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        if (storage == null) {
            final GcsClientConfig clientConfig = new GcsClientConfig(configs);
            final Storage gcsClient = GcsClientUtils.createStorageService(clientConfig);
            storage = new GcsStorage(gcsClient);
        }
    }

    @VisibleForTesting
    void setStorage(final GcsStorage storage) {
        this.storage = storage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GcsStorage storage() {
        return storage;
    }
}
