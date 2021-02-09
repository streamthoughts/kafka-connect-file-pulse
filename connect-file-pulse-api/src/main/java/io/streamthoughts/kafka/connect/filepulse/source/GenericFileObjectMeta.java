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

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public class GenericFileObjectMeta implements FileObjectMeta {

    private final URI uri;
    private final String name;
    private final long contentLength;
    private final long lastModified;
    private final ContentDigest contentDigest;
    private final Map<String, Object> userDefinedMetadata;

    /**
     * Creates a new {@link GenericFileObjectMeta} instance.
     *
     * @param uri                   the URI of the source object.
     * @param name                  the name of the source object.
     * @param contentLength         the content-length of the source object.
     * @param lastModified          the the creation date or the last modified date, whichever is the latest.
     * @param contentDigest         the digest of the content of the source object.
     * @param userDefinedMetadata   the user-defined metadata of the source object.
     */
    @JsonCreator
    public GenericFileObjectMeta(@JsonProperty("uri") final URI uri,
                                 @JsonProperty("name") final String name,
                                 @JsonProperty("contentLength") final long contentLength,
                                 @JsonProperty("lastModified") final long lastModified,
                                 @JsonProperty("contentDigest") final ContentDigest contentDigest,
                                 @JsonProperty("userDefinedMetadata") final Map<String, Object> userDefinedMetadata) {
        this.uri = uri;
        this.name = name;
        this.contentLength = contentLength;
        this.lastModified = lastModified;
        this.contentDigest = contentDigest;
        this.userDefinedMetadata = userDefinedMetadata;
    }

    public void addUserDefinedMetadata(final String key, final Object value) {
        this.userDefinedMetadata.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URI uri() {
        return uri;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return name;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public long contentLength() {
        return contentLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long lastModified() {
        return lastModified;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContentDigest contentDigest() {
        return contentDigest;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> userDefinedMetadata() {
        return userDefinedMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericFileObjectMeta)) return false;
        GenericFileObjectMeta that = (GenericFileObjectMeta) o;
        return contentLength == that.contentLength &&
                lastModified == that.lastModified &&
                Objects.equals(uri, that.uri) &&
                Objects.equals(name, that.name) &&
                Objects.equals(contentDigest, that.contentDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, name, contentLength, lastModified, contentDigest);
    }

    @Override
    public String toString() {
        return "[" +
                "uri=" + uri +
                ", name='" + name + '\'' +
                ", contentLength=" + contentLength +
                ", lastModified=" + lastModified +
                ", contentDigest=" + contentDigest +
                ", userDefinedMetadata=" + userDefinedMetadata +
                ']';
    }
}
