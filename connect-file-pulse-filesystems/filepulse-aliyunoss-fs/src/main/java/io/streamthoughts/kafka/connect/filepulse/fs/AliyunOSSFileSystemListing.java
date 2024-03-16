/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import static io.streamthoughts.kafka.connect.filepulse.internal.StringUtils.isNotBlank;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;
import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.fs.utils.AliyunOSSClientUtils;
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
 * The {@code AliyunOSSFileSystemListing} that can be used for
 * listing objects that exist in a specific Aliyun OSS bucket.
 */
public class AliyunOSSFileSystemListing implements FileSystemListing<AliyunOSSStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunOSSFileSystemListing.class);

    private FileListFilter filter;
    private AliyunOSSClientConfig config;
    private OSSClient client;
    private AliyunOSSStorage ossStorage;


    @Override
    public void configure(final Map<String, ?> configs) {
        configure(new AliyunOSSClientConfig(configs));
    }

    @VisibleForTesting
    void configure(final AliyunOSSClientConfig config) {
        this.config = config;
        this.client = AliyunOSSClientUtils.createOSSClient(config);
        this.ossStorage = new AliyunOSSStorage(client);
        this.ossStorage.setDefaultStorageClass(config.getOSSDefaultStorageClass());
        if (!ossStorage.doesOSSBucketExist(config.getOSSBucketName())) {
            throw new ConfigException("Invalid OSS bucket name. " +
                    "Bucket does not exist, or an error happens while connecting to Amazon service");
        }
    }

    /**
     * list bucket objects
     *
     * @return
     */
    @Override
    public Collection<FileObjectMeta> listObjects() {
        final ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(config.getOSSBucketName());
        if (isNotBlank(config.getOSSBucketPrefix())) {
            request.setPrefix(config.getOSSBucketPrefix());
        }
        final List<FileObjectMeta> objectMetaList = new LinkedList<>();
        ObjectListing objectListing;
        try {
            do {
                LOG.info("Sending new request for listing objects: bucketName={}, prefix={}", request.getBucketName(),
                        request.getPrefix());
                objectListing = client.listObjects(request);
                objectMetaList.addAll(objectListing.getObjectSummaries().stream()
                        .filter(ossObjectSummary -> !ossObjectSummary.getKey().startsWith("oss://"))
                        .map(ossObjectSummary -> new OSSBucketKey(ossObjectSummary.getBucketName(),
                                ossObjectSummary.getKey())).map(ossStorage::getObjectMetadata).filter(Objects::nonNull)
                        .collect(Collectors.toList()));
                String marker = objectListing.getNextMarker();
                if (marker != null) {
                    LOG.debug("Object listing is truncated, next marker is {}", marker);
                    request.setMarker(marker);
                }
            } while (objectListing.isTruncated());
        } catch (Exception e) {
            LOG.error("Failed to list objects from the Aliyun OSS bucket '{}'. " +
                    "Error occurred while processing the request: {}", config.getOSSBucketName(), e);
        }
        return filter == null ? objectMetaList : filter.filterFiles(objectMetaList);
    }


    @Override
    public void setFilter(final FileListFilter filter) {
        this.filter = filter;
    }

    @Override
    public AliyunOSSStorage storage() {
        return ossStorage;
    }
}
