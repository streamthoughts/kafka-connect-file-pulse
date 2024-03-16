/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class FixedSchemaConverter extends AbstracConnectSchemaConverter {

    static final String CONNECT_AVRO_FIXED_SIZE_PROP = "connect.fixed.size";

    FixedSchemaConverter() {}

    /** {@inheritDoc} **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {

        final SchemaBuilder builder = SchemaBuilder.bytes();
        builder.parameter(CONNECT_AVRO_FIXED_SIZE_PROP, String.valueOf(schema.getFixedSize()));

        addSchemaMetadata(schema, options, builder);

        return builder;
    }
}
