/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RecordSchemaConverter extends AbstracConnectSchemaConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordSchemaConverter.class);

    RecordSchemaConverter() {}

    /**
     * {@inheritDoc}
     **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {
        final SchemaBuilder builder;
        builder = SchemaBuilder.struct();
        context.cycleReferences().put(schema, new CyclicSchemaWrapper(builder));

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            Object defaultVal = null;
            try {
                defaultVal = field.defaultVal();
            } catch (Exception e) {
                LOG.warn("Ignoring invalid default for field " + field, e);
            }

            Options fieldOptions = options
                    .forceOptional(false)
                    .fieldDefaultValue(defaultVal);

            org.apache.kafka.connect.data.Schema fieldSchema = toConnectSchemaWithCycles(
                    field.schema(),
                    fieldOptions,
                    context
            );
            builder.field(field.name(), fieldSchema);
        }

        addSchemaMetadata(schema, options, builder);

        if (!context.detectedCycles().contains(schema)
                && context.cycleReferences().containsKey(schema)) {
            context.cycleReferences().remove(schema);
        }

        return builder;

    }
}
