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
