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
package io.streamthoughts.kafka.connect.filepulse.source.internal;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.InvalidRecordException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

public class InternalSourceRecordBuilder {

    private String topic;
    private Integer partition;
    private Supplier<SchemaAndValue> keySupplier;
    private Supplier<SchemaAndValue> valueSupplier;
    private Long timestamp;
    private ConnectHeaders additionalHeaders;

    /**
     * Creates a new {@link InternalSourceRecordBuilder} instance.
     */
    public InternalSourceRecordBuilder() {}

    public SourceRecord build(final Map<String, ?> sourcePartition,
                              final Map<String, ?> sourceOffset,
                              final FileObjectMeta metadata,
                              final String defaultTopic,
                              final Integer defaultPartition) {
        Objects.requireNonNull(sourcePartition, "sourcePartition cannot be null");
        Objects.requireNonNull(sourceOffset, "sourceOffset cannot be null");
        Objects.requireNonNull(metadata, "metadata cannot be null");

        final SchemaAndValue key = keySupplier != null ? keySupplier.get() : null;
        final SchemaAndValue value = valueSupplier != null ? valueSupplier.get() : null;

        if (key == null && value == null) {
            throw new InvalidRecordException("key and value cannot be both null");
        }

        final ConnectHeaders headers = metadata.toConnectHeader();
        if (additionalHeaders != null) {
            additionalHeaders.forEach(headers::add);
        }

        return new SourceRecord(
            sourcePartition,
            sourceOffset,
            topic != null ? topic : defaultTopic,
            partition != null ? partition : defaultPartition,
            key != null ? key.schema() : null,
            key != null ? key.value() : null,
            value != null ? value.schema() : null,
            value != null ? value.value() : null,
            timestamp,
            headers
        );
    }

    public String topic() {
        return topic;
    }

    public Integer partition() {
        return partition;
    }

    public void withValue(final Supplier<SchemaAndValue> valueSupplier) {
        this.valueSupplier = valueSupplier;
    }

    public void withKey(final Supplier<SchemaAndValue> keySupplier) {
        this.keySupplier = keySupplier;
    }

    public void withTopic(final String topic) {
        this.topic = topic;
    }

    public void withPartition(final Integer partition) {
        this.partition = partition;
    }

    public void withTimestamp(final Long timestamp) {
        this.timestamp = timestamp;
    }

    public void withHeaders(final ConnectHeaders headers) {
        this.additionalHeaders = headers;
    }
}
