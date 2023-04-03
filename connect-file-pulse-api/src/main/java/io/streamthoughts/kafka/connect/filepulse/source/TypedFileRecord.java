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

import static io.streamthoughts.kafka.connect.filepulse.internal.StringUtils.isNotBlank;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.schema.SchemaMerger;
import io.streamthoughts.kafka.connect.filepulse.source.internal.ConnectSchemaMapper;
import io.streamthoughts.kafka.connect.filepulse.source.internal.InternalSourceRecordBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

public class TypedFileRecord extends AbstractFileRecord<TypedStruct> {

    public static final String DEFAULT_MESSAGE_FIELD = "message";

    private final InternalSourceRecordBuilder internalSourceRecordBuilder;

    private final ConnectSchemaMapper mapper = new ConnectSchemaMapper();

    /**
     * Creates a new {@link TypedFileRecord} instance.
     *
     * @param offset the {@link FileRecordOffset} instance.
     * @param struct the {@link TypedStruct} instance.
     */
    public TypedFileRecord(final FileRecordOffset offset,
                           final TypedStruct struct) {
        super(offset, struct);
        internalSourceRecordBuilder = new InternalSourceRecordBuilder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SourceRecord toSourceRecord(final Map<String, ?> sourcePartition,
                                       final Map<String, ?> sourceOffset,
                                       final FileObjectMeta metadata,
                                       final String defaultTopic,
                                       final Integer defaultPartition,
                                       final Function<String, Schema> connectSchemaSupplier,
                                       final ConnectSchemaMapperOptions options) {
        mapper.setKeepLeadingUnderscoreCharacters(options.isKeepSchemaLeadingUnderscore());

        String recordTopic = internalSourceRecordBuilder.topic();

        final String targetTopic = isNotBlank(recordTopic) ? recordTopic : defaultTopic;

        Schema connectSchema = Optional
                .ofNullable(connectSchemaSupplier.apply(targetTopic))
                .orElse(connectSchemaSupplier.apply(defaultTopic));

        // Check if the connectSchema must be used for this topic
        Pattern topicPattern = options.getConnectSchemaConditionTopicPattern();
        if (topicPattern != null) {
            Matcher matcher = topicPattern.matcher(targetTopic);
            if (!matcher.matches()) {
                connectSchema = null;
            }
        }

        final Schema valueSchema;
        final TypedStruct value = value();
        if (options.isConnectSchemaMergeEnabled() && value != null) {
            // Convert dynamic StrutSchema to static Connect Schema
            Schema recordValueSchema = value.schema().map(mapper, false);
            if (connectSchema != null) {
                // Merge Record Connect Schema with either the previous one or the one
                // passed through the connector's config.
                valueSchema = SchemaMerger.merge(connectSchema, recordValueSchema);
            } else {
                // Use the Record Connect Schema
                valueSchema = recordValueSchema;
            }
        } else {
            valueSchema = connectSchema;
        }

        if (valueSchema != null) {
            internalSourceRecordBuilder.withValue(() ->
                    value == null ? null : mapper.map(valueSchema, value)
            );
        } else {
            internalSourceRecordBuilder.withValue(() ->
                    value == null ? null : mapper.map(value.schema(), value, false)
            );
        }

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
                    if (key == null || key.isNull()) {
                        return null;
                    }
                    return key.schema().map(mapper, key.value(), false);
                }
        );
        return this;
    }
}
