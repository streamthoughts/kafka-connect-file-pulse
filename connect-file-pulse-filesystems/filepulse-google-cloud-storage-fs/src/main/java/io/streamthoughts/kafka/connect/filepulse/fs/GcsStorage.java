/*
 * Copyright 2021 StreamThoughts.
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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Storage} implementation for Google Cloud Storage.
 */
public class GcsStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(GcsStorage.class);

    private final com.google.cloud.storage.Storage storage;

    /**
     * Creates a new {@link GcsStorage} instance.
     * @param storage   the {@link com.google.cloud.storage.Storage} storage.
     */
    public GcsStorage(final com.google.cloud.storage.Storage storage) {
        this.storage = storage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta getObjectMetadata(final URI uri) {
        try {
            return createFileObjectMeta(getBlob(uri));
        } catch (IOException e) {
            throw new ConnectFilePulseException("Failed to get Blob metadata for uri: " + uri, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final URI uri) {
        try {
            return existsBlob(getBlob(uri));
        } catch (IOException e) {
            throw new ConnectFilePulseException("Failed to check if Blob exists for uri: " + uri, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(final URI uri) {
        try {
            return getBlob(uri).delete();
        } catch (IOException e) {
            LOG.error("Failed to delete Blob for uri: {}", uri, e);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean move(final URI source, final URI dest) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream(final URI uri) {
        final Blob blob;
        try {
            blob = getBlob(uri);
        } catch (IOException e) {
            throw new ConnectFilePulseException("Failed to get Blob for uri: " + uri, e);
        }
        ReadChannel channel = blob.reader();
        return Channels.newInputStream(channel);
    }

    @VisibleForTesting
    Blob getBlob(final URI uri) throws IOException {
        try {
            return storage.get(new GCSBlobURI(uri).getBlobId());
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    private boolean existsBlob(final Blob blob) {
        return blob != null && blob.exists();
    }

    public static FileObjectMeta createFileObjectMeta(final Blob blob) {

        final HashMap<String, Object> userDefinedMetadata = new HashMap<>();

        userDefinedMetadata.put("gcs.blob.bucket", blob.getBucket());
        userDefinedMetadata.put("gcs.blob.name", blob.getName());

        Optional.ofNullable(blob.getMetadata())
                .ifPresent(it -> it.forEach((k, v) -> userDefinedMetadata.put("gcs.blob.user.metadata." + k, v)));
        Optional.ofNullable(blob.getEtag())
                .ifPresent(it -> userDefinedMetadata.put("gcs.blob.etag", it));
        Optional.ofNullable(blob.getStorageClass())
                .ifPresent(it -> userDefinedMetadata.put("gcs.blob.storageClass", it.name()));
        Optional.ofNullable(blob.getContentEncoding())
                .ifPresent(it -> userDefinedMetadata.put("gcs.blob.contentEncoding", it));
        Optional.ofNullable(blob.getContentType())
                .ifPresent(it -> userDefinedMetadata.put("gcs.blob.contentType", it));
        Optional.ofNullable(blob.getCreateTime())
                .ifPresent(it -> userDefinedMetadata.put("gcs.blob.createTime", it));
        Optional.ofNullable(blob.getOwner())
                .ifPresent(it -> userDefinedMetadata.put("gcs.blob.ownerType", it.getType()));

        return new GenericFileObjectMeta.Builder()
                .withUri(createBlobURI(blob))
                .withName(blob.getName())
                .withContentLength(blob.getSize())
                .withLastModified(blob.getUpdateTime())
                .withContentDigest(getContentDigestOrNull(blob))
                .withUserDefinedMetadata(userDefinedMetadata)
                .build();
    }

    private static URI createBlobURI(final Blob blob) {
        Objects.requireNonNull(blob, "blob should not be null");
        return createBlobURI(blob.getBucket(), blob.getName());
    }

    public static URI createBlobURI(final String bucketName,
                                    final String name) {
        return new GCSBlobURI(bucketName, name).getURI();
    }

    private static FileObjectMeta.ContentDigest getContentDigestOrNull(final Blob blob) {
        final String crc32c = blob.getCrc32c();
        final String md5 = blob.getMd5();
        if (crc32c != null)
            return new FileObjectMeta.ContentDigest(crc32c, "CRC32");
        else if (md5 != null)
            return new FileObjectMeta.ContentDigest(md5, "MD5");

        return null;
    }

    static class GCSBlobURI {

        public static final String GCS_URI_SCHEME = "gcs://";
        public static final String URI_SEPARATOR = "/";

        private final String bucketName;
        private final String blobName;

        public GCSBlobURI(final String bucket, final String name) {
            this.bucketName = Objects.requireNonNull(bucket, "'bucket cannot be null'");
            this.blobName = Objects.requireNonNull(name, "'name cannot be null'");
        }

        public GCSBlobURI(final URI uri) {
            if (!GCS_URI_SCHEME.startsWith(uri.getScheme())) {
                throw new IllegalArgumentException("Invalid URI scheme: " + uri.getScheme());
            }

            bucketName = uri.getAuthority();
            if (bucketName == null) {
                throw new IllegalArgumentException("Invalid GCS URI: no bucket: " + uri);
            }
            URI baseURI = URI.create(GCS_URI_SCHEME + bucketName);
            blobName = baseURI.relativize(uri).getPath();
        }

        /**
         * @return the URI of the blob the format: gcs://bucketName/blobName.
         */
        public URI getURI() {
            return URI.create(GCS_URI_SCHEME + bucketName + URI_SEPARATOR + blobName);
        }

        /**
         * @return the name of the bucket that contains the blob
         */
        public String getBucketName() {
            return bucketName;
        }

        /**
         * @return the name of the blob
         */
        public String getBlobName() {
            return blobName;
        }

        public BlobId getBlobId() {
            return BlobId.of(bucketName, blobName);
        }
    }
}
