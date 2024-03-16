/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro.internal;

import org.apache.avro.Schema;
import org.apache.kafka.connect.errors.DataException;

public final class UnionSchemaConverter extends AbstracConnectSchemaConverter {

    private static final org.apache.avro.Schema NULL_AVRO_SCHEMA =
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);

    UnionSchemaConverter() {}

    /**
     * {@inheritDoc}
     **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {
        // handle NULLABLE field with default to null
        if (schema.getTypes().size() == 2) {
            if (schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
                for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
                    if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                        Options memberOptions = options
                                .forceOptional(true)
                                .fieldDefaultValue(null);
                        return toConnectSchemaWithCycles(
                                memberSchema,
                                memberOptions,
                                context
                        );
                    }
                }
            }
        }
        throw new DataException("Couldn't translate multiple union type.");
    }
}
