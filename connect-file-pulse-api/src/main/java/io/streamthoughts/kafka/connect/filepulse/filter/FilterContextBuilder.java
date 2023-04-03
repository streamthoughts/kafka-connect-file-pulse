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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TimestampedRecordOffset;
import java.util.Map;
import org.apache.kafka.connect.header.ConnectHeaders;

/**
 * Class which is used for builder new {@link FilterContext} instance.
 */
public class FilterContextBuilder {

    private FilterError error;
    private FileObjectMeta metadata;
    private FileRecordOffset offset;
    private String topic;
    private Integer partition;
    private Long timestamp;
    private String key;
    private ConnectHeaders headers;
    private Map<String, Object> variables;

    /**
     * Creates a new {@link FilterContextBuilder} instance.
     * @return a new {@link FilterContextBuilder}.
     */
    static FilterContextBuilder newBuilder() {
        return new FilterContextBuilder();
    }

    /**
     * Creates a new builder based on the specified original {@link FilterContext} instance.
     *
     * @param original  the original {@link FilterContext} instance.
     * @return          a new {@link FilterContextBuilder}.
     */
    static FilterContextBuilder newBuilder(final FilterContext original) {
        return new FilterContextBuilder()
                .withTopic(original.topic())
                .withKey(original.key())
                .withPartition(original.partition())
                .withOffset(original.offset())
                .withTimestamp(original.timestamp())
                .withMetadata(original.metadata())
                .withOffset(original.offset())
                .withVariables(original.variables())
                .withHeaders(original.headers())
                .withError(original.error());
    }

    FilterContextBuilder withVariables(final Map<String, Object> variables) {
        this.variables = variables;
        return this;
    }

    FilterContextBuilder withTopic(final String topic) {
        this.topic = topic;
        return this;
    }

    FilterContextBuilder withPartition(final Integer partition) {
        this.partition = partition;
        return this;
    }

    FilterContextBuilder withTimestamp(final Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    FilterContextBuilder withKey(final String key) {
        this.key = key;
        return this;
    }

    FilterContextBuilder withHeaders(final ConnectHeaders headers) {
        this.headers = headers;
        return this;
    }

    FilterContextBuilder withMetadata(final FileObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    FilterContextBuilder withOffset(final FileRecordOffset offset) {
        this.offset = offset;
        return this;
    }

    FilterContextBuilder withError(final FilterError exception) {
        this.error = exception;
        return this;
    }

    public FilterContext build() {
        if (timestamp == null && offset instanceof TimestampedRecordOffset) {
            timestamp = ((TimestampedRecordOffset)offset).timestamp();
        }
        return new InternalFilterContext(
                metadata,
                offset,
                topic,
                partition,
                timestamp,
                key,
                headers,
                error,
                variables);
    }
}
