/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class TypedFileRecordTest {

    public static final String DEFAULT_TOPIC_TEST = "default";
    public static final int DEFAULT_PARTITION_TOPIC = 0;
    public static final SchemaBuilder DEFAULT_CONNECT_SCHEMA = SchemaBuilder.struct()
            .field("f2", SchemaBuilder.string().optional().build());

    @Test
    void should_merge_record_schema_given_no_pattern() {
        // Given
        TypedStruct struct = TypedStruct.create()
                .put("f1", "foo");

        TypedFileRecord record = new TypedFileRecord(FileRecordOffset.invalid(), struct);

        Map<String, Schema> connectSchemaSupplier = new HashMap<>();
        connectSchemaSupplier.put(DEFAULT_TOPIC_TEST, DEFAULT_CONNECT_SCHEMA);

        var options = new FileRecord.ConnectSchemaMapperOptions(
                true,
                true,
                null
        );

        // When
        SourceRecord sourceRecord = getSourceRecord(record, connectSchemaSupplier, options);

        // Then
        Assertions.assertNotNull(sourceRecord);
        Schema valueSchema = sourceRecord.valueSchema();
        Assertions.assertNotNull(valueSchema.field("f1"));
        Assertions.assertNotNull(valueSchema.field("f2"));
    }

    @Test
    void should_not_merge_record_schema_given_pattern() {
        // Given
        TypedStruct struct = TypedStruct.create()
                .put("f1", "foo");

        TypedFileRecord record = new TypedFileRecord(FileRecordOffset.invalid(), struct);

        Map<String, Schema> connectSchemaSupplier = new HashMap<>();
        connectSchemaSupplier.put(DEFAULT_TOPIC_TEST, DEFAULT_CONNECT_SCHEMA);

        var options = new FileRecord.ConnectSchemaMapperOptions(
                true,
                true,
                Pattern.compile(".*")
        );

        // When
        SourceRecord sourceRecord = getSourceRecord(record, connectSchemaSupplier, options);

        // Then
        Assertions.assertNotNull(sourceRecord);
        Schema valueSchema = sourceRecord.valueSchema();
        Assertions.assertNotNull(valueSchema.field("f1"));
        Assertions.assertNotNull(valueSchema.field("f2"));
    }

    private static SourceRecord getSourceRecord(TypedFileRecord record,
                                                Map<String, Schema> connectSchemaSupplier,
                                                FileRecord.ConnectSchemaMapperOptions options) {
        return record.toSourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                new GenericFileObjectMeta(URI.create("file://test")),
                DEFAULT_TOPIC_TEST,
                DEFAULT_PARTITION_TOPIC,
                connectSchemaSupplier::get,
                options
        );
    }
}