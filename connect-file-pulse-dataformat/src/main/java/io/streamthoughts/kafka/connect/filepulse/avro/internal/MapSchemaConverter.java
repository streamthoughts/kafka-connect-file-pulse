/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class MapSchemaConverter extends AbstracConnectSchemaConverter {

    MapSchemaConverter() {}

    /**
     * {@inheritDoc}
     **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {

        Schema valueSchema = schema.getValueType();

        org.apache.kafka.connect.data.Schema avroValueSchema = toConnectSchemaWithCycles(
                valueSchema,
                options,
                context
        );

        SchemaBuilder builder = SchemaBuilder.map(STRING_SCHEMA, avroValueSchema);

        addSchemaMetadata(schema, options, builder);

        return builder;
    }
}
