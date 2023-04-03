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

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

public abstract class AbstracConnectSchemaConverter implements ConnectSchemaConverter {

    public static final String NAMESPACE = "io.streamthoughts.connect.avro";

    public static final String DEFAULT_SCHEMA_NAME = "ConnectDefault";

    public static final String DEFAULT_SCHEMA_FULL_NAME = NAMESPACE + "." + DEFAULT_SCHEMA_NAME;

    static final String AVRO_PROP = "avro";

    protected void addSchemaMetadata(Schema schema,
                                     Options options,
                                     SchemaBuilder builder) {
        // Doc
        String docVal = options.docDefaultValue() != null ? options.docDefaultValue() : schema.getDoc();
        if (docVal != null) {
            builder.doc(docVal);
        }

        // Parameters
        for (Map.Entry<String, Object> entry : schema.getObjectProps().entrySet()) {
            if (entry.getKey().startsWith(AVRO_PROP)) {
                builder.parameter(entry.getKey(), entry.getValue().toString());
            }
        }

        // Default
        if (options.fieldDefaultValue() != null) {
            builder.defaultValue(options.fieldDefaultValue());
        }

        String name = null;
        if (schema.getType() == Schema.Type.RECORD
                || schema.getType() == Schema.Type.ENUM
                || schema.getType() == Schema.Type.FIXED) {
            name = schema.getName();
        }

        if (name != null && !name.startsWith(DEFAULT_SCHEMA_FULL_NAME)) {
            if (builder.name() != null) {
                if (!name.equals(builder.name())) {
                    throw new DataException("Mismatched names: name already added to SchemaBuilder ("
                            + builder.name()
                            + ") differs from name in source schema ("
                            + name + ")");
                }
            } else {
                builder.name(name);
            }
        }

        if (options.forceOptional()) {
            builder.optional();
        }
    }

    protected org.apache.kafka.connect.data.Schema toConnectSchemaWithCycles(Schema schema,
                                                                             Options options,
                                                                             CyclicContext context) {
        org.apache.kafka.connect.data.Schema resolvedSchema;
        if (context.cycleReferences().containsKey(schema)) {
            context.detectedCycles().add(schema);
            resolvedSchema = cyclicSchemaWrapper(context.cycleReferences(), schema, options.forceOptional());
        } else {
            ConnectSchemaConverter converter = ConnectSchemaConverters.forType(schema.getType());
            resolvedSchema = converter.toConnectSchema(schema, options, context);
        }
        return resolvedSchema;
    }

    private CyclicSchemaWrapper cyclicSchemaWrapper(
            Map<org.apache.avro.Schema, CyclicSchemaWrapper> toConnectCycles,
            org.apache.avro.Schema memberSchema,
            boolean optional) {
        return new CyclicSchemaWrapper(toConnectCycles.get(memberSchema).schema(), optional);
    }

}
