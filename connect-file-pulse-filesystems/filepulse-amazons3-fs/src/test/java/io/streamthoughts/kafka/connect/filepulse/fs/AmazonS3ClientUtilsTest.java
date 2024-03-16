/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AmazonS3ClientUtilsTest{

    @Test
    public void should_return_credentials_provider_pass_though_configuration() {
        AmazonS3ClientConfig config = new AmazonS3ClientConfig(Map.of(
            AmazonS3ClientConfig.AWS_S3_BUCKET_NAME_CONFIG,"TEST",
            AmazonS3ClientConfig.AWS_CREDENTIALS_PROVIDER_CLASS, InstanceProfileCredentialsProvider.class.getName()
        ));
        AWSCredentialsProvider provider = AmazonS3ClientUtils.newCredentialsProvider(config);
        Assert.assertEquals(InstanceProfileCredentialsProvider.class, provider.getClass());
    }
}