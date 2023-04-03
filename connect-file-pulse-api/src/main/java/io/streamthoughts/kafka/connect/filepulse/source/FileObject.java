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
import com.jsoniter.annotation.JsonIgnore;
import com.jsoniter.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@code FileObject} describes an input file being processing by the connector and its current state.
 */
public final class FileObject implements Serializable {

    @JsonIgnore
    private final transient FileObjectKey key;
    private final FileObjectMeta metadata;
    private final FileObjectOffset offset;
    private final FileObjectStatus status;

    /**
     * Creates a new {@link FileObject} instance.
     *
     * @param metadata  the object file's metadata.
     * @param offset    the object file's offset.
     * @param status    the object file's status.
     */
    @JsonCreator
    public FileObject(@JsonProperty("metadata") final FileObjectMeta metadata,
                      @JsonProperty("offset") final FileObjectOffset offset,
                      @JsonProperty("status") final FileObjectStatus status) {
        this(metadata, offset, status, null);
    }

    public FileObject(final FileObjectMeta metadata,
                      final FileObjectOffset offset,
                      final FileObjectStatus status,
                      final FileObjectKey key) {
        this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
        this.offset =  Objects.requireNonNull(offset, "offset can't be null");
        this.status =  Objects.requireNonNull(status, "status can't be null");
        this.key = key;
    }

    public FileObjectMeta metadata() {
        return metadata;
    }

    public FileObjectOffset offset() {
        return offset;
    }

    public FileObjectStatus status() {
        return status;
    }

    public Optional<FileObjectKey> key() {
        return Optional.ofNullable(key);
    }

    public FileObject withStatus(final FileObjectStatus status) {
        return new FileObject(metadata, offset, status);
    }

    public FileObject withKey(final FileObjectKey key) {
        return new FileObject(metadata, offset, status, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileObject)) return false;
        FileObject that = (FileObject) o;
        return Objects.equals(metadata, that.metadata) &&
               Objects.equals(status, that.status) &&
               Objects.equals(offset, that.offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(metadata, offset, status);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "source=" + metadata +
                ", offset=" + offset +
                ", status=" + status +
                ']';
    }
}
