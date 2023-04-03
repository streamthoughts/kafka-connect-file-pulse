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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.internal.Environment;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.header.ConnectHeaders;

/**
 * Simple {@link FilterContext} implementation class.
 */
public class InternalFilterContext extends Environment implements FilterContext {

    private final ConnectHeaders headers;

    private final FileObjectMeta metadata;

    private final FilterError exception;

    private final FileRecordOffset offset;

    private String topic;

    private Integer partition;

    private final Long timestamp;

    private TypedStruct value;

    private String key;

    private final Map<String, Object> variables;

    /**
     * Creates a new {@link InternalFilterContext} instance.
     *
     * @param metadata  the {@link LocalFileObjectMeta} instance.
     * @param offset    the {@link FileRecordOffset} instance.
     * @param topic     the topic to be used for source-record - may be {@code null}.
     * @param partition the partition to be used for source-record - may be {@code null}.
     * @param timestamp the timestamp to be used for source-record - may be {@code null}.
     * @param key       the key to be used for source-record - may be {@code null}.
     * @param headers   the headers to be added for source-record - may be {@code null}.
     * @param exception the record-source target topic (can be {@code null}).
     * @param variables the variables attached to this context.
     */
    InternalFilterContext(final FileObjectMeta metadata,
                          final FileRecordOffset offset,
                          final String topic,
                          final Integer partition,
                          final Long timestamp,
                          final String key,
                          final ConnectHeaders headers,
                          final FilterError exception,
                          final Map<String, Object> variables) {
        Objects.requireNonNull(metadata, "metadata can't be null");
        Objects.requireNonNull(offset, "offset can't be null");
        this.metadata = metadata;
        this.offset = offset;
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.key = key;
        this.exception = exception;
        this.headers = headers == null ? new ConnectHeaders() : headers;
        this.variables = variables == null ? new HashMap<>() : new HashMap<>(variables);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta metadata() {
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileRecordOffset offset() {
        return offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectHeaders headers() {
        return headers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer partition() {
        return partition;
    }

    public void setPartition(final Integer partition) {
        this.partition = partition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String topic() {
        return topic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String key() {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long timestamp() {
        return timestamp;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public TypedStruct value() {
        return value;
    }

    public void setValue(final TypedStruct value){
        this.value = value;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FilterError error() {
        return exception;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> variables() {
        return variables;
    }
}
