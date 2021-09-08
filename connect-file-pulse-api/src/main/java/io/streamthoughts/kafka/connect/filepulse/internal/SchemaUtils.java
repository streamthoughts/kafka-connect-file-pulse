/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.internal;

import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utility methods to manipulate Connect {@link Schema}.
 */
public class SchemaUtils {

    /**
     * @return the merged of two given {@link Schema}.
     */
    public static Schema merge(final Schema left, final Schema right) {

        if (left.equals(right)) return left;

        if (left.type() == Type.ARRAY ||
            right.type() == Type.ARRAY) {
            return mergeArray(left, right);
        }

        // Struct cannot only be merged with another Struct
        if (left.type() == Type.STRUCT &&
            right.type() == Type.STRUCT
        ) {
            return mergeStruct(left, right);
        }

        // Map cannot only be merged with another Map
        if (left.type() == Type.MAP &&
            right.type() == Type.MAP) {
            return mergeMap(left, right);
        }

        if (left.type() == right.type()) {
            return left;
        }

        if (left.type() == Type.STRING || right.type() == Type.STRING)
            return mergeMetadata(left, right, SchemaBuilder.string());

        if ( (left.type() == Type.INT64 && right.type() == Type.INT32) ||
             (right.type() == Type.INT64 && left.type() == Type.INT32)) {
            return mergeMetadata(left, right, SchemaBuilder.int64());
        }

        if ( (left.type() == Type.FLOAT64 && isNumber(right.type())) ||
             (right.type() == Type.FLOAT64 && isNumber(left.type()))) {
            return mergeMetadata(left, right, SchemaBuilder.float64());
        }

        throw new DataException("Cannot merge incompatible schema type " + left.type() + "<>" + right.type());
    }

    private static Schema mergeMap(final Schema left, final Schema right) {
        final SchemaBuilder merged = SchemaBuilder.map(
                merge(left.keySchema(), right.keySchema()),
                merge(left.valueSchema(), right.valueSchema())
        );

        return mergeMetadata(left, right, merged).build();
    }

    private static Schema mergeArray(final Schema left, final Schema right) {
        final Schema valueSchema;

        // Merge Array<?> with Array<?>
        if (left.type() == Type.ARRAY &&
            right.type() == Type.ARRAY) {
            valueSchema = merge(left.valueSchema(), right.valueSchema());

        // Merge Array<?> with ?
        } else if (left.type() == Type.ARRAY) {
            valueSchema = merge(left.valueSchema(), right);

        // Merge ? with Array<?>
        } else {
            valueSchema = merge(left, right.valueSchema());
        }
        return SchemaBuilder
                .array(valueSchema)
                .optional()
                .defaultValue(null)
                .build();
    }

    private static Schema mergeStruct(final Schema left, final Schema right) {

        if (!Objects.equals(left.name(), right.name()))
            throw new DataException(
                    "Cannot merge two schemas wih different name " + left.name() + "<>" + right.name());

        final SchemaBuilder merged = mergeMetadata(left, right, new SchemaBuilder(Type.STRUCT));

        final Map<String, Schema> remaining = left.fields()
                .stream()
                .collect(Collectors.toMap(Field::name, Field::schema));

        // Iterator on RIGHT fields and compare to LEFT fields.
        for (final Field rightField : right.fields()) {

            final String name = rightField.name();

            // field exist only on RIGHT schema.
            if (!remaining.containsKey(name)) {
                merged.field(name, rightField.schema());
                continue;
            }

            // field exist on both LEFT and RIGHT schemas.
            final Schema leftSchema = remaining.remove(name);

            try {
                final Schema fieldMergedSchema = merge(leftSchema, rightField.schema());
                merged.field(name, fieldMergedSchema);
            } catch (Exception e) {
                throw new DataException("Failed to merge schemas for field '" + name + "'. ", e);
            }
        }

        // remaining fields that existing only on LEFT schema.
        remaining.forEach(merged::field);

        return merged;
    }

    private static SchemaBuilder mergeMetadata(final Schema left,
                                               final Schema right,
                                               final SchemaBuilder merged) {

        merged.name(left.name());
        merged.doc(left.doc());

        if (left.isOptional() || right.isOptional()) {
            merged.optional();
        }

        if (left.defaultValue() != null) {
            merged.defaultValue(left.defaultValue());
        } else if (right.defaultValue() != null) {
            merged.defaultValue(right.defaultValue());
        }

        final Map<String, String> parameters = new HashMap<>();
        if (left.parameters() != null) {
            parameters.putAll(left.parameters());
        }

        if (right.parameters() != null) {
            parameters.putAll(right.parameters());
        }

        if (!parameters.isEmpty()) {
            merged.parameters(parameters);
        }

        return merged;
    }

    private static boolean isInteger(final Type type) {
        return type == Type.INT8 ||
               type == Type.INT16 ||
               type == Type.INT32 ||
               type == Type.INT64;
    }

    private static boolean isNumber(final Type type) {
        return isInteger(type) || Arrays.asList(Type.FLOAT32, Type.FLOAT64).contains(type);
    }
}