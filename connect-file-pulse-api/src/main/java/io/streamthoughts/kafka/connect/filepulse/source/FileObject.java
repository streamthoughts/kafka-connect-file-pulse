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

import java.io.Serializable;
import java.util.Objects;

/**
 * A {@code FileObject} describes an input file being processing by the connector and its current state.
 */
public class FileObject implements Serializable {

    private final FileObjectMeta metadata;
    private final FileObjectOffset offset;
    private final FileObjectStatus status;

    /**
     * Creates a new {@link FileObject} instance.
     *
     * @param source    the source file metadata.
     * @param offset    the source file offset.
     * @param status    the source status.
     */
    @JsonCreator
    public FileObject(@JsonProperty("metadata") final FileObjectMeta source,
                      @JsonProperty("offset") final FileObjectOffset offset,
                      @JsonProperty("status") final FileObjectStatus status) {
        Objects.requireNonNull(source, "source can't be null");
        Objects.requireNonNull(offset, "offset can't be null");
        Objects.requireNonNull(status, "status can't be null");
        this.metadata = source;
        this.offset = offset;
        this.status = status;
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

    public FileObject withStatus(final FileObjectStatus status) {
        return new FileObject(metadata, offset, status);
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
