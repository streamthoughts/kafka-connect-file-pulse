/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import io.streamthoughts.kafka.connect.filepulse.avro.UnsupportedAvroTypeException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

public final class ConnectSchemaConverters {

    private static final Map<Schema.Type, ConnectSchemaConverter> CONVERTERS =
            new EnumMap<>(Schema.Type.class);

    static {
        CONVERTERS.put(Schema.Type.BYTES, new BytesSchemaConverter());
        CONVERTERS.put(Schema.Type.FIXED, new FixedSchemaConverter());
        CONVERTERS.put(Schema.Type.ARRAY, new ArraySchemaConverter());
        CONVERTERS.put(Schema.Type.MAP, new MapSchemaConverter());
        CONVERTERS.put(Schema.Type.RECORD, new RecordSchemaConverter());
        CONVERTERS.put(Schema.Type.UNION, new UnionSchemaConverter());
        CONVERTERS.put(Schema.Type.LONG, new LongSchemaConverter());
        CONVERTERS.put(Schema.Type.INT, new IntSchemaConverter());
        CONVERTERS.put(Schema.Type.DOUBLE, connectSchemaConverter(SchemaBuilder::float64));
        CONVERTERS.put(Schema.Type.FLOAT,connectSchemaConverter(SchemaBuilder::float32));
        CONVERTERS.put(Schema.Type.BOOLEAN,connectSchemaConverter(SchemaBuilder::bool));
        CONVERTERS.put(Schema.Type.ENUM,connectSchemaConverter(SchemaBuilder::string));
        CONVERTERS.put(Schema.Type.STRING,connectSchemaConverter(SchemaBuilder::string));
        CONVERTERS.put(Schema.Type.NULL,
                (schema, options, context) -> {
                    throw new DataException("Standalone null schemas are not supported.");
                });
    }

    private static AbstracConnectSchemaConverter connectSchemaConverter(Supplier<SchemaBuilder> supplier) {
        return new AbstracConnectSchemaConverter() {
            @Override
            public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                        Options options,
                                                                        CyclicContext context) {
                SchemaBuilder builder = supplier.get();
                addSchemaMetadata(schema, options, builder);
                return builder;
            }
        };
    }

    public static ConnectSchemaConverter forType(final Schema.Type type) {
        ConnectSchemaConverter converter = CONVERTERS.get(type);
        return Optional.ofNullable(converter)
                .orElseThrow(
                        () ->
                                new UnsupportedAvroTypeException(
                                        "Cannot convert to connect schema. to Avro data, type is not"
                                                + " supported '"
                                                + type
                                                + "'"));
    }
}
