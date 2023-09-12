/*
 * Copyright 2023 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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