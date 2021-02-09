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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.source.internal.ConnectSchemaMapper;
import io.streamthoughts.kafka.connect.filepulse.source.internal.InternalSourceRecordBuilder;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class TypedFileRecord extends AbstractFileRecord<TypedStruct> {

    public static final String DEFAULT_MESSAGE_FIELD = "message";

    private final InternalSourceRecordBuilder internalSourceRecordBuilder;

    /**
     * Creates a new {@link TypedFileRecord} instance.
     *
     * @param offset    the {@link FileRecordOffset} instance.
     * @param struct    the {@link TypedStruct} instance.
     */
    public TypedFileRecord(final FileRecordOffset offset,
                           final TypedStruct struct) {
        super(offset, struct);
        this.internalSourceRecordBuilder = new InternalSourceRecordBuilder(
            () -> struct.schema().map(ConnectSchemaMapper.INSTANCE, struct)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SourceRecord toSourceRecord(final Map<String, ?> sourcePartition,
                                       final Map<String, ?> sourceOffset,
                                       final FileObjectMeta metadata,
                                       final String defaultTopic,
                                       final Integer defaultPartition) {
        return internalSourceRecordBuilder.build(
            sourcePartition,
            sourceOffset,
            metadata,
            defaultTopic,
            defaultPartition
        );
    }

    public TypedFileRecord withTopic(final String topic) {
        internalSourceRecordBuilder.withTopic(topic);
        return this;
    }

    public TypedFileRecord withPartition(final Integer partition) {
        internalSourceRecordBuilder.withPartition(partition);
        return this;
    }

    public TypedFileRecord withTimestamp(final Long timestamp) {
        internalSourceRecordBuilder.withTimestamp(timestamp);
        return this;
    }

    public TypedFileRecord withHeaders(final ConnectHeaders headers) {
        internalSourceRecordBuilder.withHeaders(headers);
        return this;
    }

    public TypedFileRecord withKey(final TypedValue key) {
        internalSourceRecordBuilder.withKey(
            () -> {
                if (key == null || key.isNull() ) {
                    return null;
                }
                return key.schema()
                          .map(ConnectSchemaMapper.INSTANCE, key.value());
            }
        );
        return this;
    }
}
