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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StorageClass;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AmazonS3Storage} can be used to interact with Amazon S3.
 */
public class AmazonS3Storage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonS3Storage.class);

    private StorageClass defaultStorageClass;
    private final AmazonS3 s3Client;

    /**
     * Creates a new {@link AmazonS3Storage} instance.
     *
     * @param s3Client the Amazon S3 client.
     */
    public AmazonS3Storage(final AmazonS3 s3Client) {
        this.s3Client = Objects.requireNonNull(s3Client, "s3Client should not be null");
    }

    public void setDefaultStorageClass(final StorageClass defaultStorageClass) {
        this.defaultStorageClass = defaultStorageClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final URI objectURI) {
        return doesS3ObjectExist(S3BucketKey.fromURI(objectURI));
    }

    /**
     * {@inheritDoc}
     */
    public boolean delete(final URI objectURI) {
        final S3BucketKey s3Object = S3BucketKey.fromURI(objectURI);
        try {
            this.s3Client.deleteObject(s3Object.bucketName(), s3Object.key());
            return true;
        } catch (AmazonServiceException e) {
            LOG.error(
                    "Failed to remove object from Amazon S3. "
                    + "Error occurred while processing the request for {}: {}",
                    s3Object.toURI(),
                    e
            );
        } catch (SdkClientException e) {
            LOG.error(
                    "Failed to remove object from Amazon S3. "
                    + "Error occurred while making the request or handling the response for {}: {}",
                    s3Object.toURI(),
                    e
            );
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean move(final URI source, final URI dest) {
        final S3BucketKey s3SourceObject = S3BucketKey.fromURI(source);
        final S3BucketKey s3DestinationObject = S3BucketKey.fromURI(dest);
        try {
            // AWS S3 does not support built-in move operation.
            // Move should be implemented as copy+delete
            final CopyObjectRequest copyObjectRequest = new CopyObjectRequest(
                    s3SourceObject.bucketName(),
                    s3SourceObject.key(),
                    s3DestinationObject.bucketName(),
                    s3DestinationObject.key()
            );

            // Load metadata for retrieving storage class of source object.
            ObjectMetadata objectMetadata = loadObjectMetadata(s3SourceObject);

            if (this.defaultStorageClass != null) {
                copyObjectRequest.setStorageClass(this.defaultStorageClass);
            } else {
                String storageClass = objectMetadata.getStorageClass();
                if (storageClass != null) {
                    copyObjectRequest.setStorageClass(storageClass);
                }
            }

            // Copy to target using object metadata from source object
            LOG.debug(
                    "Copying S3 object from {} to {}",
                    s3SourceObject.toURI(),
                    s3DestinationObject.toURI()
                    );
            CopyObjectResult objectResult = this.s3Client.copyObject(copyObjectRequest);
            if (objectResult.getETag() != null) {
                LOG.debug("Deleting S3 object: {}", s3SourceObject.toURI());
                return delete(s3SourceObject.toURI());
            }
            return false;
        } catch (AmazonServiceException e) {
            LOG.error(
                    "Failed to move object from Amazon S3 to {}. "
                  + "Error occurred while processing the request for {}: {}",
                    s3DestinationObject.toURI(),
                    s3SourceObject.toURI(),
                    e
            );
        } catch (SdkClientException e) {
            LOG.error(
                    "Failed to move object from Amazon S3 to {}. "
                    + "Error occurred while making the request or handling the response for {}: {}",
                    s3DestinationObject.toURI(),
                    s3SourceObject.toURI(),
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
        final S3BucketKey s3Object = S3BucketKey.fromURI(objectURI);
        final GetObjectRequest request = new GetObjectRequest(s3Object.bucketName(), s3Object.key());
        return s3Client.getObject(request).getObjectContent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta getObjectMetadata(final URI objectURI) {
        final AmazonS3URI amazonS3URI = new AmazonS3URI(objectURI);
        return getObjectMetadata(amazonS3URI.getBucket(), amazonS3URI.getKey());

    }

    public FileObjectMeta getObjectMetadata(final String bucketName, final String key) {
        return getObjectMetadata(new S3BucketKey(bucketName, key));
    }

    public FileObjectMeta getObjectMetadata(final S3BucketKey s3Object) {
        ObjectMetadata objectMetadata = loadObjectMetadata(s3Object);
        return createFileObjectMeta(
                s3Object,
                objectMetadata
        );
    }

    private ObjectMetadata loadObjectMetadata(final S3BucketKey s3Object) {
        var request = new GetObjectMetadataRequest(s3Object.bucketName(), s3Object.key());
        try {
           return s3Client.getObjectMetadata(request);
        } catch (AmazonServiceException e) {
            LOG.error(
                    "Failed to get object metadata from Amazon S3. "
                    + "Error occurred while processing the request for {}: {}",
                    s3Object.toURI(),
                    e
            );
            throw e;
        } catch (SdkClientException e) {
            LOG.error(
                    "Failed to get object metadata from Amazon S3. "
                    + "Error occurred while making the request or handling the response for {}: {}",
                    s3Object.toURI(),
                    e
            );
            throw e;
        }
    }

    public boolean doesS3BucketExist(final String bucketName) {
        try {
            return s3Client.doesBucketExistV2(bucketName);
        } catch (AmazonServiceException e) {
            LOG.error(
                    "Failed to check if Amazon S3 bucket '{}' exist. "
                    + "Error occurred while processing the request: {}",
                    bucketName,
                    e
            );
        } catch (SdkClientException e) {
            LOG.error(
                    "Failed to check if Amazon S3 bucket '{}' exist. "
                    + "Error occurred while making the request or handling the response: {}",
                    bucketName,
                    e
            );
        }
        return false;
    }

    public boolean doesS3ObjectExist(final S3BucketKey s3Object) {
        try {
            return s3Client.doesObjectExist(s3Object.bucketName(), s3Object.key());
        } catch (AmazonServiceException e) {
            LOG.error(
                    "Failed to check if object with key '{}' exist on Amazon S3 bucket '{}'. "
                            + "Error occurred while processing the request: {}",
                    s3Object.key(),
                    s3Object.bucketName(),
                    e
            );
        } catch (SdkClientException e) {
            LOG.error(
                    "Failed to check if object with key '{}' exist on Amazon S3 bucket '{}'. "
                            + "Error occurred while making the request or handling the response: {}",
                    s3Object.key(),
                    s3Object.bucketName(),
                    e
            );
        }
        return false;
    }

    private static FileObjectMeta createFileObjectMeta(final S3BucketKey s3Object,
                                                       final ObjectMetadata objectMetadata) {

        final HashMap<String, Object> userDefinedMetadata = new HashMap<>();

        objectMetadata.getUserMetadata().forEach((k, v) -> userDefinedMetadata.put("s3.object.user.metadata." + k, v));
        userDefinedMetadata.put("s3.object.summary.bucketName", s3Object.bucketName());
        userDefinedMetadata.put("s3.object.summary.key", s3Object.key());
        userDefinedMetadata.put("s3.object.summary.etag", objectMetadata.getETag());
        userDefinedMetadata.put("s3.object.summary.storageClass", objectMetadata.getStorageClass());

        final String contentMD5 = objectMetadata.getContentMD5();

        FileObjectMeta.ContentDigest digest = null;
        if (contentMD5 != null) {
            digest = new FileObjectMeta.ContentDigest(contentMD5, "MD5");
        }

        return new GenericFileObjectMeta.Builder()
                .withUri(s3Object.toURI())
                .withName(s3Object.key())
                .withContentLength(objectMetadata.getContentLength())
                .withLastModified(objectMetadata.getLastModified())
                .withContentDigest(digest)
                .withUserDefinedMetadata(userDefinedMetadata)
                .build();
    }
}
