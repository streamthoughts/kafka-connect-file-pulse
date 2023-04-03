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

import static java.util.Objects.requireNonNull;

import com.jsoniter.annotation.JsonCreator;
import com.jsoniter.annotation.JsonProperty;
import io.streamthoughts.kafka.connect.filepulse.internal.Network;
import java.io.Serializable;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.header.ConnectHeaders;

/**
 * An object regrouping metadata about an input file manipulate by the connector.
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
    Long contentLength();

    /**
     * @return the creation date or the last modified date, whichever is the latest.
     */
    @JsonProperty("lastModified")
    Long lastModified();

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

        Optional.ofNullable(contentLength())
                .ifPresent(val -> headers.addLong("connect.file.contentLength", val));

        Optional.ofNullable(lastModified())
                .ifPresent(val -> headers.addLong("connect.file.lastModified", val));

        userDefinedMetadata().forEach( (k, v) -> {
            if (v != null) {
                headers.addString("connect.file." + k, v.toString());
            }
        });
        headers.addString("connect.task.hostname", Network.HOSTNAME);
        if (contentDigest() != null) {
            headers.addString("connect.file.hash.digest", contentDigest().digest());
            headers.addString("connect.file.hash.algorithm", contentDigest().algorithm());
        }
        return headers;
    }

    @Override
    default int compareTo(final FileObjectMeta that) {
        return Long.compare(this.lastModified(), that.lastModified());
    }

    class ContentDigest {
        private final String digest;
        private final String algorithm;

        @JsonCreator
        public ContentDigest(@JsonProperty("digest") final String digest,
                             @JsonProperty("algorithm") final String algorithm) {
            this.digest = requireNonNull(digest, "'digest' should not be null");;
            this.algorithm = requireNonNull(algorithm, "'algorithm' should not be null")
                    .toUpperCase(Locale.getDefault());
        }

        @JsonProperty("digest")
        public String digest() {
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
            return Objects.equals(digest, that.digest) &&
                    Objects.equals(algorithm, that.algorithm);
        }

        @Override
        public int hashCode() {
            return Objects.hash(digest, algorithm);
        }

        @Override
        public String toString() {
            return "[" +
                    "digest=" + digest +
                    ", algorithm='" + algorithm + '\'' +
                    ']';
        }
    }
}
