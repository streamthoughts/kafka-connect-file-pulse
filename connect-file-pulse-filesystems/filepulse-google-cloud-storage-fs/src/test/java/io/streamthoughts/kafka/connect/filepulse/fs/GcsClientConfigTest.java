/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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