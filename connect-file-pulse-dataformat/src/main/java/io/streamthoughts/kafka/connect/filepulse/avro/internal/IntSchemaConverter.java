/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;

public final class IntSchemaConverter extends AbstracConnectSchemaConverter {
    static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";

    static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    static final String AVRO_LOGICAL_DATE = "date";

    IntSchemaConverter() {}

    /** {@inheritDoc} **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {
        final SchemaBuilder builder;

        String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);
        if (logicalType == null) {
            builder = SchemaBuilder.int32();
        } else {
            if (AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
                builder = Date.builder();
            } else if (AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
                builder = Time.builder();
            } else {
                builder = SchemaBuilder.int32();
            }
        }

        addSchemaMetadata(schema, options, builder);

        return builder;
    }
}
