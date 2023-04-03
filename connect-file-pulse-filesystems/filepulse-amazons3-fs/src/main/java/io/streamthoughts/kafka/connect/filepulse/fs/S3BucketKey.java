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

import static io.streamthoughts.kafka.connect.filepulse.internal.StringUtils.substringAfterLast;

import com.amazonaws.services.s3.AmazonS3URI;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.net.URI;
import java.util.Objects;

/**
 * This class represents an object stored in Amazon S3. This object contains
 * the object key and the Amazon S3 bucket name.
 */
public class S3BucketKey {

    public static final String S3_FOLDER_SEPARATOR = "/";
    private final String bucketName;
    private final String key;

    /**
     * An helper method to create a new {@link S3BucketKey} from a given {@link URI}.
     *
     * @param uri   the uri.
     * @return      a new {@link S3BucketKey}.
     */
    public static S3BucketKey fromURI(final URI uri) {
        final AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        return new S3BucketKey(
            amazonS3URI.getBucket(),
            amazonS3URI.getKey()
        );
    }

    public S3BucketKey(final String bucketName,
                       final String keyPrefix,
                       final String keyName) {
        this(bucketName, StringUtils.removeEnd(keyPrefix, S3_FOLDER_SEPARATOR) + S3_FOLDER_SEPARATOR + keyName);
    }

    /**
     * Creates a new {@link S3BucketKey} instance.
     *
     * @param bucketName    The Amazon S3 bucket name.
     * @param key           The S3 object key.
     */
    public S3BucketKey(final String bucketName, final String key) {
        this.bucketName = Objects.requireNonNull(bucketName, "bucketName should not be null");
        this.key = Objects.requireNonNull(key, "key should not be null");;
    }

    /**
     * @return the Amazon S3 bucket name.
     */
    public String bucketName() {
        return bucketName;
    }

    /**
     * @return the Amazon S3 object key.
     */
    public String key() {
        return key;
    }

    public String objectName() {
        return substringAfterLast(key, S3_FOLDER_SEPARATOR.charAt(0));
    }
    /**
     * @return the {@link URI} for this Amazon S3 object.
     */
    public URI toURI() {
        AmazonS3URI s3Uri = new AmazonS3URI("s3://" + bucketName + S3_FOLDER_SEPARATOR + key, true);
        return s3Uri.getURI();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3BucketKey)) return false;
        S3BucketKey s3Object = (S3BucketKey) o;
        return Objects.equals(bucketName, s3Object.bucketName) &&
                Objects.equals(key, s3Object.key);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(bucketName, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "S3Object{" +
                "bucketName='" + bucketName + '\'' +
                ", key='" + key + '\'' +
                '}';
    }
}
