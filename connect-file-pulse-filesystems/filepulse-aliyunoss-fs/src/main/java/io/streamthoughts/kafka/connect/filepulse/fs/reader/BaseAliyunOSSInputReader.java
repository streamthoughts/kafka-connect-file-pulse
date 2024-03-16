/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import com.aliyun.oss.OSSClient;
import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.fs.AliyunOSSClientConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.AliyunOSSStorage;
import io.streamthoughts.kafka.connect.filepulse.fs.utils.AliyunOSSClientUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code BaseAliyunOSSInputReader} provides the {@link AliyunOSSStorage}.
 */
public abstract class BaseAliyunOSSInputReader extends AbstractFileInputReader
        implements StorageAwareFileInputReader<AliyunOSSStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(BaseAliyunOSSInputReader.class);
    protected AliyunOSSClientConfig clientConfig;
    private OSSClient ossClient;
    private AliyunOSSStorage storage;

    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        if (storage == null) {
            LOG.info("Create new Aliyun OSS client from the properties passed through the connector's configuration ");
            this.clientConfig = new AliyunOSSClientConfig(configs);
            ossClient = AliyunOSSClientUtils.createOSSClient(clientConfig);
            storage = new AliyunOSSStorage(ossClient);
            storage.setDefaultStorageClass(clientConfig.getOSSDefaultStorageClass());
        }
    }

    @VisibleForTesting
    void setStorage(final AliyunOSSStorage storage) {
        this.storage = storage;
    }

    @Override
    public AliyunOSSStorage storage() {
        return storage;
    }

    @Override
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }
}
