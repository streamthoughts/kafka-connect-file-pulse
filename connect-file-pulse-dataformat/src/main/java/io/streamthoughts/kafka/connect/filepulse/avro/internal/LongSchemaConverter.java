/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

public final class LongSchemaConverter extends AbstracConnectSchemaConverter {

    static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";

    LongSchemaConverter() {}

    /** {@inheritDoc} **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {
        final SchemaBuilder builder;

        String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);
        if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
            builder = Timestamp.builder();
        } else {
            builder = SchemaBuilder.int64();
        }

        addSchemaMetadata(schema, options, builder);

        return builder;
    }
}
