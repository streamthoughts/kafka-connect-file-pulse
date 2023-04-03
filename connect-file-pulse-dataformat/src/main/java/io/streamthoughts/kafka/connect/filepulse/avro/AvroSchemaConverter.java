/*
 * Copyright 2023 StreamThoughts.
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