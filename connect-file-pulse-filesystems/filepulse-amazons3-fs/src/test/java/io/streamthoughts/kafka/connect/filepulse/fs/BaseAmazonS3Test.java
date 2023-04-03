/*
 * Copyright 2022 StreamThoughts.
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

import static io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientConfig.AWS_S3_REGION_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientConfig.AWS_SECRET_ACCESS_KEY_CONFIG;

import com.amazonaws.services.s3.AmazonS3;
import io.findify.s3mock.S3Mock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;

public class BaseAmazonS3Test {

    public static final String S3_TEST_BUCKET = "testbucket";
    protected S3Mock s3Mock;
    protected AmazonS3 client;
    protected String endpointConfiguration;
    protected Map<String, String> unmodifiableCommonsProperties;

    @Before
    public void setUp() throws Exception {
        final Random generator = new Random();
        final int s3Port = generator.nextInt(10000) + 10000;
        s3Mock = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Mock.start();

        endpointConfiguration = "http://localhost:" + s3Port;
        unmodifiableCommonsProperties = new HashMap<>();
        unmodifiableCommonsProperties.put(AWS_ACCESS_KEY_ID_CONFIG, "test_key_id");
        unmodifiableCommonsProperties.put(AWS_SECRET_ACCESS_KEY_CONFIG, "test_secret_key");
        unmodifiableCommonsProperties.put(AWS_S3_BUCKET_NAME_CONFIG, S3_TEST_BUCKET);
        unmodifiableCommonsProperties.put(AWS_S3_REGION_CONFIG, "us-west-2");
        unmodifiableCommonsProperties = Collections.unmodifiableMap(unmodifiableCommonsProperties);

        client = AmazonS3ClientUtils.createS3Client(
            new AmazonS3ClientConfig(unmodifiableCommonsProperties),
            endpointConfiguration
        );
    }

    @After
    public void tearDown() throws Exception {
        client.shutdown();
        s3Mock.shutdown();
    }
}
