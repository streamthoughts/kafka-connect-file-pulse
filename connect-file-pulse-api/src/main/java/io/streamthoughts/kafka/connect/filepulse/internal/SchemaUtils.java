/*
 * Copyright 2019 StreamThoughts.
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
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility methods to manipulate Connect {@link Schema}.
 */
public class SchemaUtils {

    public static List<String> getAllFieldNames(final Schema schema) {
        return schema.fields().stream().map(Field::name).collect(Collectors.toList());
    }

    public static SchemaBuilder copySchemaBasics(final Schema source) {
        return copySchemaBasics(source, new SchemaBuilder(source.type()));
    }

    public static SchemaBuilder copySchemaBasics(final Schema source, final SchemaBuilder builder) {
        builder.name(source.name());
        builder.version(source.version());
        builder.doc(source.doc());

        final Map<String, String> params = source.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        return builder;
    }

    public static void merge(final Schema leftSchema,
                             final Schema rightSchema,
                             final SchemaBuilder builder,
                             final Set<String> overwrite) {
        final Map<String, Field> rightFields = groupFieldByName(rightSchema.fields());
        for (Field leftField : leftSchema.fields()) {
            final String leftFieldName = leftField.name();

            boolean isOverwrite = overwrite.contains(leftFieldName);

            final Field rightField = rightFields.get(leftFieldName);

            if (rightField == null) {
                builder.field(leftFieldName, leftField.schema());
                continue;
            }

            if (isOverwrite) {
                continue; // skip the left field
            }

            checkIfFieldsCanBeMerged(leftField, rightField);

            if (isTypeOf(leftField, Schema.Type.ARRAY)) {
                builder.field(leftFieldName, leftField.schema());
            } else {
                final SchemaBuilder optionalArray = SchemaBuilder
                        .array(leftField.schema())
                        .optional();
                builder.field(leftFieldName, optionalArray);
            }
            rightFields.remove(leftFieldName);
        }
        // Copy all remaining fields from right struct.
        for (Field f : rightFields.values()) {
            Schema schema = f.schema();
            builder.field(f.name(), schema);
        }
    }

    private static void checkIfFieldsCanBeMerged(final Field left, final Field right) {
        final String name = left.name();
        if (isTypeOf(left, Schema.Type.ARRAY)) {
            Schema valueSchemaLeft = left.schema().valueSchema();
            if (isTypeOf(right, Schema.Type.ARRAY)) {
                Schema valueSchemaRight = right.schema().valueSchema();
                throwIfTypesAreNotEqual(name, valueSchemaLeft, valueSchemaRight,
                        "Cannot merge fields '%s' of type array with different value types : Array[%s]<>Array[%s]");
            } else {
                throwIfTypesAreNotEqual(name, valueSchemaLeft, right.schema(),
                        "Cannot merge fields '%s' with different array value types : Array[%s]<>%s");
            }
        } else if (isTypeOf(right, Schema.Type.ARRAY)) {
            Schema valueSchemaRight = right.schema().valueSchema();
            throwIfTypesAreNotEqual(name, left.schema(), valueSchemaRight,
                    "Cannot merge fields '%s' with different array value types : %s<>Array[%s]");
        // neither left or right is of type array
        } else {
            throwIfTypesAreNotEqual(name, left.schema(), right.schema(),
                    "Cannot merge fields '%s' with different types : %s<>%s");
        }
    }

    private static void throwIfTypesAreNotEqual(final String field,
                                                final Schema s1,
                                                final Schema s2,
                                                final String message) {
        if (!s1.type().equals(s2.type())) {
            throw new DataException(String.format(message, field, s1.type(), s2.type()));
        }
    }

    public static boolean isTypeOf(final Field field, final Schema.Type type) {
        return field.schema().type().equals(type);
    }

    public static Map<String, Field> groupFieldByName(final Collection<Field> fields) {
        return fields.stream().collect(Collectors.toMap(Field::name, f -> f));
    }
}