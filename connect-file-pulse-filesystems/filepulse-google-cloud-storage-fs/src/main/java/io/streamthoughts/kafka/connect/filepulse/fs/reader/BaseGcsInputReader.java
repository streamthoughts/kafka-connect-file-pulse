/*
 * Copyright 2021 StreamThoughts.
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
