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
package io.streamthoughts.kafka.connect.filepulse.fs;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

public class GcsClientConfigTest {

    @Test
    public void should_get_config_given_bucket_name() {
        final GcsClientConfig config = new GcsClientConfig(Map.of(
                GcsClientConfig.GCS_BUCKET_NAME_CONFIG, "test-bucket",
                GcsClientConfig.GCS_BLOBS_FILTER_PREFIX_CONFIG, "prefix"
        ));
        Assert.assertEquals("test-bucket", config.getBucketName());
        Assert.assertEquals("prefix", config.getBlobsPrefix());
    }

    @Test(expected = ConfigException.class)
    public void should_throw_exception_when_bucket_is_missing() {
        new GcsClientConfig(Collections.emptyMap());
    }

    @Test(expected = ConfigException.class)
    public void should_throw_exception_given_mutually_exclusive_options() {
        new GcsClientConfig(Map.of(
                GcsClientConfig.GCS_BUCKET_NAME_CONFIG, "test-bucket",
                GcsClientConfig.GCS_CREDENTIALS_JSON_CONFIG, "{}",
                GcsClientConfig.GCS_CREDENTIALS_PATH_CONFIG, "/tmp/path"
        ));
    }
}