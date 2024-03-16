/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
