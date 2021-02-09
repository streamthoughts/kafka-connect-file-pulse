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
package io.streamthoughts.kafka.connect.filepulse.source;

import com.jsoniter.annotation.JsonCreator;
import com.jsoniter.annotation.JsonProperty;
import io.streamthoughts.kafka.connect.filepulse.internal.Network;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * An object regrouping metadata about an input ile manipulate by the connector.
 */
public interface FileObjectMeta extends Serializable, Comparable<FileObjectMeta> {

    /**
     * @return the URI of the source object.
     */
    @JsonProperty("uri")
    URI uri();

    default String stringURI() {
        return uri().toString();
    }

    /**
     * @return the name of the source object.
     */
    @JsonProperty("name")
    String name();

    /**
     * @return the content-length of the source object.
     */
    @JsonProperty("contentLength")
    long contentLength();

    /**
     * @return the creation date or the last modified date, whichever is the latest.
     */
    @JsonProperty("lastModified")
    long lastModified();

    /**
     * @return the digest of the source object content.
     */
    @JsonProperty("contentDigest")
    ContentDigest contentDigest();

    /**
     * @return the user-defined metadata.
     */
    @JsonProperty("userDefinedMetadata")
    Map<String, Object> userDefinedMetadata();

    /**
     * Converts this {@link FileObjectMeta} to {@link ConnectHeaders}.
     * @return  a new {@link ConnectHeaders} metadata.
     */
    default ConnectHeaders toConnectHeader() {
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("connect.file.name", name());
        headers.addString("connect.file.uri", uri().toString());
        headers.addLong("connect.file.hash.digest", contentDigest().digest());
        headers.addString("connect.file.hash.algorithm", contentDigest().algorithm());
        headers.addLong("connect.file.contentLength", contentLength());
        headers.addLong("connect.file.lastModified", lastModified());
        userDefinedMetadata().forEach( (k, v) -> headers.addString("connect.file." + k, v.toString()));
        headers.addString("connect.task.hostname", Network.HOSTNAME);
        return headers;
    }

    @Override
    default int compareTo(final FileObjectMeta that) {
        return Long.compare(this.lastModified(), that.lastModified());
    }

    class ContentDigest {
        private final long digest;
        private final String algorithm;

        @JsonCreator
        public ContentDigest(@JsonProperty("digest") final long digest,
                             @JsonProperty("algorithm") final String algorithm) {
            this.digest = digest;
            this.algorithm = Objects.requireNonNull(algorithm, "algorithm should not be null");
        }

        @JsonProperty("digest")
        public long digest() {
            return digest;
        }

        @JsonProperty("algorithm")
        public String algorithm() {
            return algorithm;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ContentDigest)) return false;
            ContentDigest that = (ContentDigest) o;
            return digest == that.digest &&
                    Objects.equals(algorithm, that.algorithm);
        }

        @Override
        public int hashCode() {
            return Objects.hash(digest, algorithm);
        }

        @Override
        public String toString() {
            return "ContentDigest{" +
                    "digest=" + digest +
                    ", algorithm='" + algorithm + '\'' +
                    '}';
        }
    }
}
