/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import io.streamthoughts.kafka.connect.filepulse.fs.utils.AliyunOSSURI;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AliyunOSSStorage} can be used to interact with Aliyun OSS.
 */
public class AliyunOSSStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunOSSStorage.class);
    private final OSSClient ossClient;
    private String defaultStorageClass;

    /**
     * Creates a new {@link AliyunOSSStorage} instance.
     *
     * @param ossClient the aliyun oss client.
     */
    public AliyunOSSStorage(final OSSClient ossClient) {
        this.ossClient = Objects.requireNonNull(ossClient, "ossClient should not be null");
    }

    private static FileObjectMeta createFileObjectMeta(final OSSBucketKey ossObject,
                                                       final ObjectMetadata objectMetadata) {

        final HashMap<String, Object> userDefinedMetadata = new HashMap<>();
        objectMetadata.getUserMetadata().forEach((k, v) -> userDefinedMetadata.put("oss.object.user.metadata." + k, v));
        userDefinedMetadata.put("oss.object.summary.bucketName", ossObject.bucketName());
        userDefinedMetadata.put("oss.object.summary.key", ossObject.key());
        userDefinedMetadata.put("oss.object.summary.etag", objectMetadata.getETag());
        userDefinedMetadata.put("oss.object.summary.storageClass", objectMetadata.getObjectStorageClass());
        final String contentMD5 = objectMetadata.getContentMD5();
        FileObjectMeta.ContentDigest digest = null;
        if (contentMD5 != null) {
            digest = new FileObjectMeta.ContentDigest(contentMD5, "MD5");
        }
        return new GenericFileObjectMeta.Builder()
                .withUri(ossObject.toURI())
                .withName(ossObject.key())
                .withContentLength(objectMetadata.getContentLength())
                .withLastModified(objectMetadata.getLastModified())
                .withContentDigest(digest)
                .withUserDefinedMetadata(userDefinedMetadata)
                .build();
    }

    public void setDefaultStorageClass(final String defaultStorageClass) {
        this.defaultStorageClass = defaultStorageClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final URI objectURI) {
        return doesOSSObjectExist(OSSBucketKey.fromURI(objectURI));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(final URI objectURI) {
        final OSSBucketKey ossBucketKey = OSSBucketKey.fromURI(objectURI);
        try {
            this.ossClient.deleteObject(ossBucketKey.bucketName(), ossBucketKey.key());
            return true;
        } catch (Exception e) {
            LOG.error(
                    "Failed to remove object from Aliyun OSS. "
                            + "Error occurred while processing the request for {}: {}",
                    ossBucketKey.toURI(),
                    e
            );
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean move(final URI source, final URI dest) {
        final OSSBucketKey ossSourceObject = OSSBucketKey.fromURI(source);
        final OSSBucketKey ossDestinationObject = OSSBucketKey.fromURI(dest);
        try {
            // OSS does not support built-in move operation.
            // Move should be implemented as copy+delete
            final CopyObjectRequest copyObjectRequest = new CopyObjectRequest(
                    ossSourceObject.bucketName(),
                    ossSourceObject.key(),
                    ossDestinationObject.bucketName(),
                    ossDestinationObject.key()
            );
            if (!StringUtils.isBlank(defaultStorageClass)) {
                copyObjectRequest.addHeader("x-oss-storage-class", defaultStorageClass);
            }
            // Copy to target using object metadata from source object
            LOG.debug(
                    "Copying OSS object from {} to {}",
                    ossSourceObject.toURI(),
                    ossDestinationObject.toURI()
            );
            //
            CopyObjectResult objectResult = this.ossClient.copyObject(copyObjectRequest);
            if (objectResult.getETag() != null) {
                LOG.debug("Deleting OSS object: {}", ossSourceObject.toURI());
                return delete(ossSourceObject.toURI());
            }
            return false;
        } catch (Exception e) {
            LOG.error(
                    "Failed to move object from Aliyun OSS to {}. "
                            + "Error occurred while processing the request for {}: {}",
                    ossDestinationObject.toURI(),
                    ossSourceObject.toURI(),
                    e
            );
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream(final URI objectURI) {
        final OSSBucketKey ossObject = OSSBucketKey.fromURI(objectURI);
        final GetObjectRequest request = new GetObjectRequest(ossObject.bucketName(), ossObject.key());
        return ossClient.getObject(request).getObjectContent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta getObjectMetadata(final URI objectURI) {
        final AliyunOSSURI aliyunOSSURI = new AliyunOSSURI(objectURI);
        return getObjectMetadata(aliyunOSSURI.getBucket(), aliyunOSSURI.getKey());

    }

    /**
     * @param bucketName
     * @param key
     * @return
     */
    public FileObjectMeta getObjectMetadata(final String bucketName, final String key) {
        return getObjectMetadata(new OSSBucketKey(bucketName, key));
    }

    public FileObjectMeta getObjectMetadata(final OSSBucketKey ossObject) {
        ObjectMetadata objectMetadata = loadObjectMetadata(ossObject);
        return createFileObjectMeta(
                ossObject,
                objectMetadata
        );
    }

    private ObjectMetadata loadObjectMetadata(final OSSBucketKey ossObject) {
        GenericRequest request = new GenericRequest(ossObject.bucketName(), ossObject.key());
        try {
            return ossClient.getObjectMetadata(request);
        } catch (Exception e) {
            LOG.error(
                    "Failed to get object metadata from Aliyun OSS. "
                            + "Error occurred while processing the request for {}: {}",
                    ossObject.toURI(),
                    e
            );
            throw e;
        }
    }

    public boolean doesOSSBucketExist(final String bucketName) {
        try {
            return ossClient.doesBucketExist(bucketName);
        } catch (Exception e) {
            LOG.error(
                    "Failed to check if Aliyun OSS bucket '{}' exist. "
                            + "Error occurred while processing the request: {}",
                    bucketName,
                    e
            );
        }
        return false;
    }

    public boolean doesOSSObjectExist(final OSSBucketKey ossObject) {
        try {
            return ossClient.doesObjectExist(ossObject.bucketName(), ossObject.key());
        } catch (Exception e) {
            LOG.error(
                    "Failed to check if object with key '{}' exist on Aliyun OSS bucket '{}'. "
                            + "Error occurred while processing the request: {}",
                    ossObject.key(),
                    ossObject.bucketName(),
                    e
            );
        }
        return false;
    }
}
