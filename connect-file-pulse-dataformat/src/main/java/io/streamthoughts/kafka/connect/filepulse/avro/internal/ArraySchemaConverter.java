/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class ArraySchemaConverter extends AbstracConnectSchemaConverter {

    ArraySchemaConverter(){}

    /** {@inheritDoc} **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {
        final SchemaBuilder builder;

        Schema elementType = schema.getElementType();

        org.apache.kafka.connect.data.Schema avroElementSchema = toConnectSchemaWithCycles(
                elementType,
                options,
                context
        );
        builder = SchemaBuilder.array(avroElementSchema);
        addSchemaMetadata(schema, options, builder);

        return builder;
    }
}
