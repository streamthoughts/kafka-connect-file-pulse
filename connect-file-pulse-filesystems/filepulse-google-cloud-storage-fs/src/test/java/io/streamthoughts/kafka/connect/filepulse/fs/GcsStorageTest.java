/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GcsStorageTest {

    private static final String TEST_BUCKET_NAME = "TEST_BUCKET";

    private GcsStorage storage;
    public static final String TEST_BLOB_PATH = "test/path/foo.txt";
    public static final URI TEST_BLOB_URI = URI.create("gcs://" + TEST_BUCKET_NAME + "/" + TEST_BLOB_PATH);

    @Before
    public void setUp() {
        Storage service = LocalStorageHelper.getOptions().getService();
        BlobInfo blobInfo = BlobInfo
                .newBuilder(BlobId.of(TEST_BUCKET_NAME, TEST_BLOB_PATH))
                .build();
        service.create(blobInfo, "randomContent".getBytes(StandardCharsets.UTF_8));
        storage = new GcsStorage(service);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void should_get_input_stream_given_valid_blob_uri() {
        final InputStream is = storage.getInputStream(TEST_BLOB_URI);
        Assert.assertNotNull(is);
    }

    @Test
    public void should_get_blob_given_valid_uri() throws IOException {
        Assert.assertNotNull(storage.getBlob(TEST_BLOB_URI));
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_given_invalid_uri_schema() throws IOException {
        storage.getBlob(URI.create("s3://bucket/blob"));
    }
}