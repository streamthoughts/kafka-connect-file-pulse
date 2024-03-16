/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

public class AliyunOSSClientConfigTest {

    @Test
    public void should_get_config() {
        final AliyunOSSClientConfig config = new AliyunOSSClientConfig(
                Map.of(AliyunOSSClientConfig.OSS_ACCESS_KEY_ID_CONFIG, "access-key",
                        AliyunOSSClientConfig.OSS_SECRET_KEY_CONFIG, "secret-key",
                        AliyunOSSClientConfig.OSS_ENDPOINT_CONFIG, "oss-endpoint",
                        AliyunOSSClientConfig.OSS_BUCKET_NAME_CONFIG, "test-bucket",
                        AliyunOSSClientConfig.OSS_BUCKET_PREFIX_CONFIG, "prefix"));
        Assert.assertEquals("access-key", config.getOSSAccessKeyId().value());
        Assert.assertEquals("secret-key", config.getOSSAccessKey().value());
        Assert.assertEquals("oss-endpoint", config.getOSSEndpoint());
        Assert.assertEquals("test-bucket", config.getOSSBucketName());
        Assert.assertEquals("prefix", config.getOSSBucketPrefix());
    }


    @Test(expected = ConfigException.class)
    public void should_throw_exception_given_mutually_exclusive_options() {
        AliyunOSSClientConfig config = new AliyunOSSClientConfig(
                Map.of(AliyunOSSClientConfig.OSS_BUCKET_NAME_CONFIG, "test-bucket",
                        AliyunOSSClientConfig.OSS_BUCKET_PREFIX_CONFIG, "prefix"));
        config.getOSSAccessKey();
    }
}