/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import static io.streamthoughts.kafka.connect.filepulse.internal.StringUtils.substringAfterLast;

import io.streamthoughts.kafka.connect.filepulse.fs.utils.AliyunOSSURI;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.net.URI;
import java.util.Objects;


public class OSSBucketKey {

    public static final String OSS_FOLDER_SEPARATOR = "/";
    private final String bucketName;
    private final String key;

    public OSSBucketKey(final String bucketName, final String keyPrefix, final String keyName) {
        this(bucketName, StringUtils.removeEnd(keyPrefix, OSS_FOLDER_SEPARATOR) + OSS_FOLDER_SEPARATOR + keyName);
    }

    /**
     * Creates a new {@link OSSBucketKey} instance.
     *
     * @param bucketName The Aliyun OSS bucket name.
     * @param key        The OSS object key.
     */
    public OSSBucketKey(final String bucketName, final String key) {
        this.bucketName = Objects.requireNonNull(bucketName, "bucketName should not be null");
        this.key = Objects.requireNonNull(key, "key should not be null");
    }

    /**
     * An helper method to create a new {@link OSSBucketKey} from a given {@link URI}.
     *
     * @param uri the uri.
     * @return a new {@link OSSBucketKey}.
     */
    public static OSSBucketKey fromURI(final URI uri) {
        final AliyunOSSURI aliyunOSSURI = new AliyunOSSURI(uri);
        return new OSSBucketKey(aliyunOSSURI.getBucket(), aliyunOSSURI.getKey());
    }

    /**
     * @return the Aliyun OSS bucket name.
     */
    public String bucketName() {
        return bucketName;
    }

    /**
     * @return the Aliyun OSS object key.
     */
    public String key() {
        return key;
    }

    public String objectName() {
        return substringAfterLast(key, OSS_FOLDER_SEPARATOR.charAt(0));
    }

    /**
     * @return the {@link URI} for this Aliyun OSS object.
     */
    public URI toURI() {
        return URI.create("oss://" + bucketName + OSS_FOLDER_SEPARATOR + key);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OSSBucketKey)) return false;
        OSSBucketKey s3Object = (OSSBucketKey) o;
        return Objects.equals(bucketName, s3Object.bucketName) && Objects.equals(key, s3Object.key);
    }


    @Override
    public int hashCode() {
        return Objects.hash(bucketName, key);
    }

    @Override
    public String toString() {
        return "S3Object{" + "bucketName='" + bucketName + '\'' + ", key='" + key + '\'' + '}';
    }
}
