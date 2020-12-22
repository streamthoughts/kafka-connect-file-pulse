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
package io.streamthoughts.kafka.connect.filepulse.source.internal;

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import io.streamthoughts.kafka.connect.filepulse.data.MapSchema;
import io.streamthoughts.kafka.connect.filepulse.data.SchemaMapper;
import io.streamthoughts.kafka.connect.filepulse.data.SchemaMapperWithValue;
import io.streamthoughts.kafka.connect.filepulse.data.SimpleSchema;
import io.streamthoughts.kafka.connect.filepulse.data.StructSchema;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConnectSchemaMapper implements SchemaMapper<Schema>, SchemaMapperWithValue<SchemaAndValue> {

    private static final Object DEFAULT_NULL_VALUE = null;

    public static final ConnectSchemaMapper INSTANCE = new ConnectSchemaMapper();
    private static final Pattern REGEX = Pattern.compile("[_\\-.]");

    static String normalizeSchemaName(final String name) {
        return Arrays
            .stream(REGEX.split(name))
            .filter(s -> !s.isEmpty())
            .map(it -> it.substring(0, 1).toUpperCase() + it.substring(1))
            .collect(Collectors.joining());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final MapSchema schema) {
        final Schema keySchema = schema.keySchema().map(this);
        final Schema valueSchema = schema.valueSchema().map(this);
        return asNullableAndOptional(SchemaBuilder.map(keySchema, valueSchema)).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final ArraySchema schema) {
        Schema valueSchema = schema.valueSchema().map(this);
        return asNullableAndOptional(SchemaBuilder.array(valueSchema)).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final StructSchema schema) {
        SchemaBuilder sb = SchemaBuilder.struct();

        String schemaName = schema.name();
        if (schemaName != null) {
            if (schema.namespace() != null) {
                schemaName = schema.namespace() + "." + schemaName;
            }
            sb.name(schemaName);
        }

        if (schema.doc() != null) {
            sb.doc(schema.doc());
        }

        for(final TypedField field : schema) {
            final io.streamthoughts.kafka.connect.filepulse.data.Schema fieldSchema = field.schema();
            // Ignore schema NULL because cannot determine the expected type.
            if (fieldSchema.type() != Type.NULL) {
                final String fieldName = field.name();
                mayUpdateSchemaName(fieldSchema, fieldName);
                sb.field(fieldName, fieldSchema.map(this)).optional();
            }
        }
        return sb.build();
    }

    private void mayUpdateSchemaName(final io.streamthoughts.kafka.connect.filepulse.data.Schema schema,
                                     final String fieldName) {
        if (schema.type() == Type.ARRAY) {
            final ArraySchema arraySchema = (ArraySchema)schema;
            mayUpdateSchemaName(arraySchema.valueSchema(), fieldName);
        }

        if (schema.type() == Type.MAP) {
            final MapSchema mapSchema = (MapSchema)schema;
            mayUpdateSchemaName(mapSchema.valueSchema(), fieldName);
        }

        if (schema.type() == Type.STRUCT) {
            final StructSchema structSchema = (StructSchema)schema;
            if (structSchema.name() == null) {
                structSchema.name(normalizeSchemaName(fieldName));
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final SimpleSchema schema) {
        return asNullableAndOptional(new SchemaBuilder(schema.type().schemaType())).build();
    }

    private static SchemaBuilder asNullableAndOptional(final SchemaBuilder sb) {
        return sb.optional().defaultValue(DEFAULT_NULL_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final MapSchema schema, final Map<String, ?> map) {
        Schema connectSchema = schema.map(this);
        return new SchemaAndValue(connectSchema, map);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final ArraySchema schema, final Collection<?> array) {
        Schema connectSchema = schema.map(this);

        return new SchemaAndValue(connectSchema, array);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final StructSchema schema, final TypedStruct struct) {
        final Schema connectSchema = schema.map(this);
        return new SchemaAndValue(connectSchema, toConnectStruct(connectSchema, struct));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final SimpleSchema schema, final Object value) {
        return new SchemaAndValue(schema.map(this), value);
    }

    private static Struct toConnectStruct(final Schema connectSchema, final TypedStruct struct) {
        final Struct connectStruct = new Struct(connectSchema);
        for (Field field : connectSchema.fields()) {

            final String fieldName = connectSchema.name();
            final boolean isOptional = field.schema().isOptional();
            if (!struct.has(field.name())) {
                if (!isOptional) {
                    throw new DataException(
                        "Failed to convert record to connect data. " +
                        "Missing required field '" + field.name() + "' for record '" + fieldName + "'"
                    );
                }
                continue;
            }

            TypedValue typed = struct.get(field.name());
            final Schema fieldSchema = field.schema();

            final Schema.Type dataSchemaType = typed.type().schemaType();

            if (fieldSchema.type() != dataSchemaType) {

                if (!fieldSchema.type().equals(Schema.Type.ARRAY)) {
                    throw new DataException("Failed to convert record field '" + fieldName + "' to connect data. " +
                        "Types do not match " + fieldSchema.type() + "<>" + typed.type());
                }

                Schema.Type arrayValueType = fieldSchema.valueSchema().type();
                if (!arrayValueType.equals(dataSchemaType)) {
                    throw new DataException("Failed to convert record field '" + fieldName + "' to connect data. " +
                        "Types do not match Array[" + arrayValueType + "]<>Array[" + typed.type() + "]");
                }
                typed = TypedValue.array(Collections.singleton(typed.value()), typed.schema());
            }
            connectStruct.put(field, toConnectObject(fieldSchema, typed));
        }
        return connectStruct;
    }

    private static Object toConnectObject(final Schema schema, final TypedValue typed) {
        if (schema.type() != typed.type().schemaType()) {
            throw new DataException("types do not match " + schema.type() + "<>" + typed.type());
        }

        if (schema.type() == Schema.Type.STRUCT) {
            return toConnectStruct(schema, typed.value());
        }

        if (schema.type() == Schema.Type.MAP) {
            final Schema connectValueSchema = schema.valueSchema();
            final Schema connectKeySchema = schema.keySchema();

            final io.streamthoughts.kafka.connect.filepulse.data.Schema valueSchema =
                    ((MapSchema) typed.schema()).valueSchema();
            final io.streamthoughts.kafka.connect.filepulse.data.Schema keySchema =
                    ((MapSchema) typed.schema()).keySchema();

            return typed.getMap().entrySet()
                .stream()
                .collect(Collectors.toMap(
                    e -> toConnectObject(connectKeySchema, TypedValue.of(e.getKey(), keySchema)),
                    e -> toConnectObject(connectValueSchema, TypedValue.of(e.getValue(), valueSchema))
                    )
                );
        }

        if (schema.type() == Schema.Type.ARRAY) {
            final Schema connectValueSchema = schema.valueSchema();

            final io.streamthoughts.kafka.connect.filepulse.data.Schema valueSchema =
                    ((ArraySchema) typed.schema()).valueSchema();

            return typed.getArray()
                .stream()
                .map(e -> toConnectObject(connectValueSchema, TypedValue.of(e, valueSchema)))
                .collect(Collectors.toList());
        }
        return typed.value();
    }
}
