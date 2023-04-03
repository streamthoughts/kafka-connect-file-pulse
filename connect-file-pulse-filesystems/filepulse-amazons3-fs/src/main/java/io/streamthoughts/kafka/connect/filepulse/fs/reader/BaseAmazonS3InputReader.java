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

import com.amazonaws.services.s3.AmazonS3;
import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientUtils;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3Storage;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code BaseAmazonS3InputReader} provides the {@link AmazonS3Storage}.
 */
public abstract class BaseAmazonS3InputReader
        extends AbstractFileInputReader
        implements StorageAwareFileInputReader<AmazonS3Storage> {

    private static final Logger LOG = LoggerFactory.getLogger(BaseAmazonS3InputReader.class);

    private AmazonS3 s3Client;
    private AmazonS3Storage storage;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        if (storage == null) {
            LOG.info("Create new Amazon S3 client from the properties passed through the connector's configuration ");
            final AmazonS3ClientConfig clientConfig = new AmazonS3ClientConfig(configs);
            s3Client = AmazonS3ClientUtils.createS3Client(clientConfig);
            storage = new AmazonS3Storage(s3Client);
            storage.setDefaultStorageClass(clientConfig.getAwsS3DefaultStorageClass());
        }
    }

    @VisibleForTesting
    void setStorage(final AmazonS3Storage storage) {
        this.storage = storage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AmazonS3Storage storage() {
        return storage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.shutdown();
        }
    }
}
