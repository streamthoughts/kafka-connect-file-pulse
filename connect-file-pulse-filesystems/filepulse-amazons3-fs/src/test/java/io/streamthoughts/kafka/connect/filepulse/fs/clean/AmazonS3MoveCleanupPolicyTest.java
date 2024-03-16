/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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