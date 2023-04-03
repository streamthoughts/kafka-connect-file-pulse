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
