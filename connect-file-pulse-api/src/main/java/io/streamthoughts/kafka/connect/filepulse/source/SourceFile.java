/*
 * Copyright 2019-2020 StreamThoughts.
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

import java.io.File;
import java.util.Map;
import java.util.Objects;

/**
 * Default class to wrap all information about a source file.
 */
public class SourceFile {

    private final SourceMetadata metadata;
    private final SourceOffset offset;
    private final SourceStatus status;

    private final Map<String, Object> props;

    /**
     * Creates a new {@link SourceFile} instance.
     *
     * @param metadata  the source file metadata.
     * @param offset    the source file offset.
     * @param status    the source status.
     * @param props     the source properties.
     */
    @JsonCreator
    public SourceFile(@JsonProperty("metadata") final SourceMetadata metadata,
                      @JsonProperty("offset") final SourceOffset offset,
                      @JsonProperty("status") final SourceStatus status,
                      @JsonProperty("props") final Map<String, Object> props) {
        Objects.requireNonNull(metadata, "metadata can't be null");
        Objects.requireNonNull(offset, "offset can't be null");
        Objects.requireNonNull(status, "status can't be null");
        this.metadata = metadata;
        this.offset = offset;
        this.status = status;
        this.props = props;
    }

    public SourceMetadata metadata() {
        return metadata;
    }

    public SourceOffset offset() {
        return offset;
    }

    public SourceStatus status() {
        return status;
    }

    public Map<String, Object> props() {
        return props;
    }

    public File file() {
        return new File(metadata.path(), metadata.name());
    }

    public SourceFile withStatus(final SourceStatus status) {
        return new SourceFile(metadata, offset, status, props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceFile)) return false;
        SourceFile that = (SourceFile) o;
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
                "metadata=" + metadata +
                ", offset=" + offset +
                ", status=" + status +
                ", props=" + props +
                ']';
    }
}
