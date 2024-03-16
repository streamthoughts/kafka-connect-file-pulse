/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import static io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3ClientConfig.AWS_S3_BUCKET_PREFIX_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

public class AmazonS3FileSystemListingTest extends BaseAmazonS3Test {

    public static final String S3_TEST_BUCKET = "testbucket";
    public static final List<String> OBJECT_KEYS = Arrays.asList("file/name/foo/1", "file/name/foo/2", "file/name/bar/3");

    @Test
    public void should_list_all_objects_given_non_empty_bucket() {
        // GIVEN
        client.createBucket(S3_TEST_BUCKET);
        OBJECT_KEYS.forEach(key -> client.putObject(S3_TEST_BUCKET, key, "contents"));

        var clientConfig = new AmazonS3ClientConfig(unmodifiableCommonsProperties);
        var listing = new AmazonS3FileSystemListing();
        listing.configure(clientConfig, endpointConfiguration);

        // WHEN
        final Collection<FileObjectMeta> objects = listing.listObjects();

        // THEN
        Assert.assertNotNull(objects);
        Assert.assertEquals(OBJECT_KEYS.size(), objects.size());
        Assert.assertEquals(OBJECT_KEYS.size(), objects.size());

        final Set<String> onlyNames = objects.stream().map(FileObjectMeta::name).collect(Collectors.toSet());
        OBJECT_KEYS.forEach(key -> Assert.assertTrue(onlyNames.contains(key)));
    }

    @Test
    public void should_list_all_objects_given_non_empty_bucket_and_prefix() {
        // GIVEN
        client.createBucket(S3_TEST_BUCKET);
        OBJECT_KEYS.forEach(key -> client.putObject(S3_TEST_BUCKET, key, "contents"));

        var properties = new HashMap<>(unmodifiableCommonsProperties);
        properties.put(AWS_S3_BUCKET_PREFIX_CONFIG, "file/name/foo");

        var clientConfig = new AmazonS3ClientConfig(properties);
        var listing = new AmazonS3FileSystemListing();
        listing.configure(clientConfig, endpointConfiguration);

        // WHEN
        final Collection<FileObjectMeta> objects = listing.listObjects();

        // THEN
        var filteredObjectKeys = OBJECT_KEYS.stream().filter(s -> s.contains("foo")).collect(Collectors.toSet());
        Assert.assertNotNull(objects);
        Assert.assertEquals(filteredObjectKeys.size(), objects.size());
        final Set<String> onlyNames = objects.stream().map(FileObjectMeta::name).collect(Collectors.toSet());
        filteredObjectKeys.forEach(key -> Assert.assertTrue(onlyNames.contains(key)));
    }

    @Test(expected = ConfigException.class)
    public void should_throw_error_given_non_existing_bucket_name() {
        // GIVEN
        var properties = new HashMap<>(unmodifiableCommonsProperties);
        properties.put(AWS_S3_BUCKET_NAME_CONFIG, "dummy");
        var clientConfig = new AmazonS3ClientConfig(properties);

        var listing = new AmazonS3FileSystemListing();

        // WHEN/THEN
        listing.configure(clientConfig, endpointConfiguration);
    }
}