/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro;

import io.streamthoughts.kafka.connect.filepulse.avro.internal.ConnectSchemaConverter;
import io.streamthoughts.kafka.connect.filepulse.avro.internal.ConnectSchemaConverters;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;

/**
 * Utilities for converting between Connect runtime data format and Avro.
 */
public final class AvroSchemaConverter {

    private final Map<org.apache.avro.Schema, Schema> toConnectSchemaCache;

    /**
     * Creates a new {@link AvroSchemaConverter} instance.
     */
    public AvroSchemaConverter() {
        toConnectSchemaCache = new HashMap<>();
    }

    /**
     * Convert the given {@link org.apache.avro.Schema} into connect one.
     *
     * @param schema the string avro schema to be converted.
     * @return {@link Schema}.
     */
    public Schema toConnectSchema(final String schema) {
        if (StringUtils.isBlank(schema)) {
            return null;
        }
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return toConnectSchema(parser.parse(schema));
    }

    /**
     * Convert the given {@link org.apache.avro.Schema} into connect one.
     *
     * @param schema the avro schema to be converted.
     * @return {@link Schema}.
     */
    public Schema toConnectSchema(final org.apache.avro.Schema schema) {
        Schema cachedSchema = toConnectSchemaCache.get(schema);
        if (cachedSchema != null) {
            return cachedSchema;
        }

        ConnectSchemaConverter converter = ConnectSchemaConverters.forType(schema.getType());
        org.apache.kafka.connect.data.Schema resultSchema = converter.toConnectSchema(
                schema,
                new ConnectSchemaConverter.Options().forceOptional(false),
                new ConnectSchemaConverter.CyclicContext()
        );
        toConnectSchemaCache.put(schema, resultSchema);
        return resultSchema;
    }
}