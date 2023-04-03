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