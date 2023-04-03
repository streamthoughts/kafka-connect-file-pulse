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
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.BaseAmazonS3Test;
import io.streamthoughts.kafka.connect.filepulse.fs.S3BucketKey;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AmazonS3MoveCleanupPolicyTest extends BaseAmazonS3Test {

    public static final String S3_TEST_BUCKET = "bucket";
    public static final String OBJECT_NAME = "object";
    public static final String S3_OBJECT_KEY = "input/" + OBJECT_NAME;

    private AmazonS3Storage storage;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        storage = new AmazonS3Storage(client);
    }

    @Test
    public void should_move_object_on_success() {
        // GIVEN
        client.createBucket(S3_TEST_BUCKET);
        client.putObject(S3_TEST_BUCKET, S3_OBJECT_KEY, "contents");

        var cleaner = new AmazonS3MoveCleanupPolicy();
        cleaner.setStorage(storage);
        cleaner.configure(Map.of(
                AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_PREFIX_PATH_CONFIG, "/success/",
                AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_PREFIX_PATH_CONFIG, "/failure/"
        ));

        // WHEN
        FileObjectMeta objectMetadata = storage.getObjectMetadata(new S3BucketKey(S3_TEST_BUCKET, S3_OBJECT_KEY));
        cleaner.onSuccess(new FileObject(
                objectMetadata,
                FileObjectOffset.empty(),
                FileObjectStatus.COMPLETED
                )
        );

        // THEN
        Assert.assertFalse(storage.exists(objectMetadata.uri()));
        Assert.assertTrue(storage.exists(new S3BucketKey(S3_TEST_BUCKET, "/success/" + OBJECT_NAME).toURI()));
    }


    @Test
    public void should_move_object_on_failure() {
        // GIVEN
        client.createBucket(S3_TEST_BUCKET);
        client.putObject(S3_TEST_BUCKET, S3_OBJECT_KEY, "contents");

        var cleaner = new AmazonS3MoveCleanupPolicy();
        cleaner.setStorage(storage);
        cleaner.configure(Map.of(
                AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_PREFIX_PATH_CONFIG, "/success/",
                AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_PREFIX_PATH_CONFIG, "/failure/"
        ));

        // WHEN
        FileObjectMeta objectMetadata = storage.getObjectMetadata(new S3BucketKey(S3_TEST_BUCKET, S3_OBJECT_KEY));
        cleaner.onFailure(new FileObject(
                        objectMetadata,
                        FileObjectOffset.empty(),
                        FileObjectStatus.COMPLETED
                )
        );

        // THEN
        Assert.assertFalse(storage.exists(objectMetadata.uri()));
        Assert.assertTrue(storage.exists(new S3BucketKey(S3_TEST_BUCKET, "/failure/" + OBJECT_NAME).toURI()));
    }
}