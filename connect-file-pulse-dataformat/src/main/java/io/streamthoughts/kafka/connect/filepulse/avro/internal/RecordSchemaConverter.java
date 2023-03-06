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
