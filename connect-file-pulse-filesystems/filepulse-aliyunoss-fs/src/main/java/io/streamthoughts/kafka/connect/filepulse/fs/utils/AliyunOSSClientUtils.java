/*
 * Copyright 2023 StreamThoughts.
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
