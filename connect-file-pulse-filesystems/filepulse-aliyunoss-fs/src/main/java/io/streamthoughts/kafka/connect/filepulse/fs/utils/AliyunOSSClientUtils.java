/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.utils;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import io.streamthoughts.kafka.connect.filepulse.fs.AliyunOSSClientConfig;

/**
 * Utility class for creating new oss client.
 */
public class AliyunOSSClientUtils {
    /**
     * Helper method to creates a new {@link OSSClient} client.
     *
     * @param config
     * @return
     */
    public static OSSClient createOSSClient(AliyunOSSClientConfig config) {
        ClientBuilderConfiguration clientConfiguration = new ClientBuilderConfiguration();
        clientConfiguration.setMaxErrorRetry(config.getOssMaxErrorRetries());
        clientConfiguration.setConnectionTimeout(config.getOSSConnectionTimeout());
        clientConfiguration.setMaxConnections(config.getOSSMaxConnections());
        clientConfiguration.setSocketTimeout(config.getOSSSocketTimeout());

        /**
         * Create oss client
         */
        return (OSSClient) new OSSClientBuilder().build(config.getOSSEndpoint(), config.getOSSAccessKeyId().value(),
                config.getOSSAccessKey().value(), clientConfiguration);
    }
}
