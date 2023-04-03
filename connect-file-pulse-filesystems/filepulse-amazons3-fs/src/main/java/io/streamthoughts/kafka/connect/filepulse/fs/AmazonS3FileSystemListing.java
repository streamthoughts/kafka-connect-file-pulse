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

import static io.streamthoughts.kafka.connect.filepulse.internal.StringUtils.isNotBlank;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AmazonS3FileSystemListing} that can be used for listing objects that exist in a specific Amazon S3 bucket.
 */
public class AmazonS3FileSystemListing implements FileSystemListing<AmazonS3Storage> {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonS3FileSystemListing.class);

    private FileListFilter filter;
    private AmazonS3ClientConfig config;
    private AmazonS3 client;
    private AmazonS3Storage s3Storage;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        configure(new AmazonS3ClientConfig(configs), null);
    }

    @VisibleForTesting
    void configure(final AmazonS3ClientConfig config, final String url) {
        this.config = config;
        this.client = AmazonS3ClientUtils.createS3Client(config, url);
        this.s3Storage = new AmazonS3Storage(client);
        this.s3Storage.setDefaultStorageClass(config.getAwsS3DefaultStorageClass());
        if (!s3Storage.doesS3BucketExist(config.getAwsS3BucketName())) {
            throw new ConfigException(
                    "Invalid S3 bucket name. "
                            + "Bucket does not exist, or an error happens while connecting to Amazon service"
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<FileObjectMeta> listObjects() {
        final ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(config.getAwsS3BucketName());

        if (isNotBlank(config.getAwsS3BucketPrefix()))
            request.setPrefix(config.getAwsS3BucketPrefix());

        final List<FileObjectMeta> objectMetaList = new LinkedList<>();
        ObjectListing objectListing;
        try {
            do {
                LOG.debug(
                        "Sending new request for listing objects: bucketName={}, prefix={}",
                        request.getBucketName(),
                        request.getPrefix()
                );
                objectListing = client.listObjects(request);

                objectMetaList.addAll(objectListing.getObjectSummaries()
                        .stream()
                        .map(s3ObjectSummary ->
                                new S3BucketKey(
                                        s3ObjectSummary.getBucketName(),
                                        s3ObjectSummary.getKey()
                                ))
                        .map(s3Storage::getObjectMetadata)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));

                String marker = objectListing.getNextMarker();
                if (marker != null) {
                    LOG.debug("Object listing is truncated, next marker is {}", marker);
                    request.setMarker(marker);
                }

            } while (objectListing.isTruncated());
        } catch (AmazonServiceException e) {
            LOG.error(
                    "Failed to list objects from the Amazon S3 bucket '{}'. "
                            + "Error occurred while processing the request: {}",
                    config.getAwsS3BucketName(),
                    e
            );
        } catch (SdkClientException e) {
            LOG.error(
                    "Failed to list objects from the Amazon S3 bucket '{}'. "
                            + "Error occurred while making the request or handling the response: {}",
                    config.getAwsS3BucketName(),
                    e
            );
        }
        return filter == null ? objectMetaList : filter.filterFiles(objectMetaList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFilter(final FileListFilter filter) {
        this.filter = filter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AmazonS3Storage storage() {
        return s3Storage;
    }
}
