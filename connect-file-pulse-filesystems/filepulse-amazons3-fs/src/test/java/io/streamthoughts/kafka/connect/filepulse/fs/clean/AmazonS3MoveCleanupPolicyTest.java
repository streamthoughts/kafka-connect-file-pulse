/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3StorageConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.BaseAmazonS3Test;
import io.streamthoughts.kafka.connect.filepulse.fs.S3BucketKey;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class AmazonS3MoveCleanupPolicyTest extends BaseAmazonS3Test {

    public static final String S3_TEST_BUCKET = "bucket";
    public static final String OBJECT_NAME = "object";
    public static final String S3_OBJECT_KEY = "input/" + OBJECT_NAME;
    public static final String S3_OBJECT_KEY_WITH_PREFIX = "input/prefix/" + OBJECT_NAME;
    public static final String EXCLUDE_SOURCE_PREFIX_PATH = "input";

    private AmazonS3Storage storage;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        storage = new AmazonS3Storage(client);
        storage.configure(new HashMap<>());
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
    public void should_move_object_on_success_with_prefix() {
        // GIVEN
        client.createBucket(S3_TEST_BUCKET);
        client.putObject(S3_TEST_BUCKET, S3_OBJECT_KEY_WITH_PREFIX, "contents");

        var cleaner = new AmazonS3MoveCleanupPolicy();
        cleaner.setStorage(storage);
        cleaner.configure(Map.of(
                AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_PREFIX_PATH_CONFIG, "/success/",
                AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_PREFIX_PATH_CONFIG, "/failure/",
                AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH, true,
                AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH, true,
                AmazonS3MoveCleanupPolicy.Config.EXCLUDE_SOURCE_PREFIX_PATH_CONFIG, EXCLUDE_SOURCE_PREFIX_PATH
        ));

        // WHEN
        FileObjectMeta objectMetadata = storage.getObjectMetadata(new S3BucketKey(S3_TEST_BUCKET, S3_OBJECT_KEY_WITH_PREFIX));
        cleaner.onSuccess(new FileObject(
                        objectMetadata,
                        FileObjectOffset.empty(),
                        FileObjectStatus.COMPLETED
                )
        );

        // THEN
        Assert.assertFalse(storage.exists(objectMetadata.uri()));
        Assert.assertTrue(storage.exists(new S3BucketKey(S3_TEST_BUCKET, "/success/prefix/" + OBJECT_NAME).toURI()));
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

    @Test
    public void should_move_object_on_failure_with_prefix() {
        // GIVEN
        client.createBucket(S3_TEST_BUCKET);
        client.putObject(S3_TEST_BUCKET, S3_OBJECT_KEY_WITH_PREFIX, "contents");

        var cleaner = new AmazonS3MoveCleanupPolicy();
        cleaner.setStorage(storage);
        cleaner.configure(Map.of(
                AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_PREFIX_PATH_CONFIG, "/success/",
                AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_PREFIX_PATH_CONFIG, "/failure/",
                AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_INCLUDE_SOURCE_PREFIX_PATH, true,
                AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_INCLUDE_SOURCE_PREFIX_PATH, true,
                AmazonS3MoveCleanupPolicy.Config.EXCLUDE_SOURCE_PREFIX_PATH_CONFIG, EXCLUDE_SOURCE_PREFIX_PATH
        ));

        // WHEN
        FileObjectMeta objectMetadata = storage.getObjectMetadata(new S3BucketKey(S3_TEST_BUCKET, S3_OBJECT_KEY_WITH_PREFIX));
        cleaner.onFailure(new FileObject(
                        objectMetadata,
                        FileObjectOffset.empty(),
                        FileObjectStatus.COMPLETED
                )
        );

        // THEN
        Assert.assertFalse(storage.exists(objectMetadata.uri()));
        Assert.assertTrue(storage.exists(new S3BucketKey(S3_TEST_BUCKET, "/failure/prefix/" + OBJECT_NAME).toURI()));
    }

    @Test
    public void should_move_object_using_multipart() {
        // GIVEN
        int fiveMB = 5 * 1024 * 1024 + 1;
        AmazonS3 spyClient = Mockito.spy(client);
        client.createBucket(S3_TEST_BUCKET);
        byte[] content = new byte[fiveMB + 1];
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(content.length);
        client.putObject(S3_TEST_BUCKET, S3_OBJECT_KEY, new ByteArrayInputStream(content), meta);

        AmazonS3Storage bigStorage = new AmazonS3Storage(spyClient);
        bigStorage.configure(Map.of(
                AmazonS3StorageConfig.MULTIPART_COPY_THRESHOLD_CONFIG, fiveMB,
                AmazonS3StorageConfig.PART_SIZE_CONFIG, fiveMB
        ));

        var cleaner = new AmazonS3MoveCleanupPolicy();
        cleaner.setStorage(bigStorage);
        cleaner.configure(Map.of(
                AmazonS3MoveCleanupPolicy.Config.SUCCESS_AWS_PREFIX_PATH_CONFIG, "/success/",
                AmazonS3MoveCleanupPolicy.Config.FAILURES_AWS_PREFIX_PATH_CONFIG, "/failure/"
        ));

        // WHEN
        FileObjectMeta objectMetadata = bigStorage.getObjectMetadata(new S3BucketKey(S3_TEST_BUCKET, S3_OBJECT_KEY));
        cleaner.onSuccess(new FileObject(objectMetadata, FileObjectOffset.empty(), FileObjectStatus.COMPLETED));

        // THEN
        Assert.assertFalse(bigStorage.exists(objectMetadata.uri()));
        Assert.assertTrue(bigStorage.exists(new S3BucketKey(S3_TEST_BUCKET, "/success/" + OBJECT_NAME).toURI()));
        Mockito.verify(spyClient, Mockito.times(1)).initiateMultipartUpload(
                Mockito.argThat(argument ->
                        argument.getBucketName().equals(S3_TEST_BUCKET) &&
                        argument.getKey().equals("/success/object")
                )
        );
        Mockito.verify(spyClient, Mockito.times(2)).copyPart(
                Mockito.argThat(argument ->
                        argument.getSourceBucketName().equals(S3_TEST_BUCKET) &&
                        argument.getSourceKey().equals(S3_OBJECT_KEY) &&
                        argument.getDestinationBucketName().equals(S3_TEST_BUCKET) &&
                        argument.getDestinationKey().equals("/success/object")
                )
        );
        Mockito.verify(spyClient, Mockito.times(1)).completeMultipartUpload(
                Mockito.argThat(argument ->
                        argument.getBucketName().equals(S3_TEST_BUCKET) &&
                                argument.getKey().equals("/success/object") &&
                                argument.getPartETags().size() == 2
                )
        );
    }
}