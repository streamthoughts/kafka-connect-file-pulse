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
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

public final class BytesSchemaConverter extends AbstracConnectSchemaConverter {

    static final String AVRO_LOGICAL_DECIMAL = "decimal";
    static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    static final Integer CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 64;

    static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

    BytesSchemaConverter() {}

    /** {@inheritDoc} **/
    @Override
    public org.apache.kafka.connect.data.Schema toConnectSchema(Schema schema,
                                                                Options options,
                                                                CyclicContext context) {
        String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);

        final SchemaBuilder builder;
        if (AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
            builder = schemaBuilderForLogicalDecimal(schema);
        } else {
            builder = SchemaBuilder.bytes();
        }

        addSchemaMetadata(schema, options, builder);

        return builder;
    }

    private static SchemaBuilder schemaBuilderForLogicalDecimal(org.apache.avro.Schema schema) {
        final SchemaBuilder builder;
        Object scaleNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP);
        // In Avro: scale, a JSON integer representing the scale. If not specified the scale is 0.
        int scale = scaleNode instanceof Number ? ((Number) scaleNode).intValue() : 0;
        builder = Decimal.builder(scale);

        Object precisionNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
        if (null != precisionNode) {
            // In Avro: precision, a JSON integer representing the (maximum) precision
            // of decimals stored in this type (required).
            if (!(precisionNode instanceof Number)) {
                throw new DataException(AVRO_LOGICAL_DECIMAL_PRECISION_PROP
                        + " property must be a JSON Integer."
                        + " https://avro.apache.org/docs/1.11.1/specification/#decimal");
            }
            // Capture the precision as a parameter only if it is not the default
            int precision = ((Number) precisionNode).intValue();
            if (precision != CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT) {
                builder.parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, String.valueOf(precision));
            }
        }
        return builder;
    }
}
