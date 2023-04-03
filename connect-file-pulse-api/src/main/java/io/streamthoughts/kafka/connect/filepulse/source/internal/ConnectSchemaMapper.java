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

import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
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
import io.streamthoughts.kafka.connect.filepulse.schema.SchemaContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectSchemaMapper implements SchemaMapper<Schema>, SchemaMapperWithValue<SchemaAndValue> {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectSchemaMapper.class);

    private static final Object DEFAULT_NULL_VALUE = null;

    private static final Pattern REGEX = Pattern.compile("[_\\-.]");

    private final SchemaContext context = new SchemaContext();

    private boolean keepLeadingUnderscoreCharacters = false;

    @VisibleForTesting
    String normalizeSchemaName(final String name) {
        String toNormalize = name;
        StringBuilder prefix = new StringBuilder();
        if (keepLeadingUnderscoreCharacters) {
            StringBuilder sb = new StringBuilder(name);
            while (sb.length() > 0 && sb.charAt(0) == '_') {
                prefix.append("_");
                sb.deleteCharAt(0);
            }
            toNormalize = sb.toString();

        }
        return prefix + Arrays
                .stream(REGEX.split(toNormalize))
                .filter(s -> !s.isEmpty())
                .map(it -> it.substring(0, 1).toUpperCase(Locale.getDefault()) + it.substring(1))
                .collect(Collectors.joining());
    }

    public void setKeepLeadingUnderscoreCharacters(final boolean keepLeadingUnderscoreCharacters) {
        this.keepLeadingUnderscoreCharacters = keepLeadingUnderscoreCharacters;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final MapSchema schema, final boolean optional) {
        final Schema keySchema = schema.keySchema().map(this, optional);
        final Schema valueSchema = schema.valueSchema().map(this, optional);

        final SchemaBuilder builder = SchemaBuilder.map(keySchema, valueSchema);
        return (optional ? asNullableAndOptional(builder) : builder).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final ArraySchema schema, final boolean optional) {
        final Schema valueSchema = schema.valueSchema().map(this, optional);

        final SchemaBuilder builder = SchemaBuilder.array(valueSchema);
        return (optional ? asNullableAndOptional(builder) : builder).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final StructSchema schema, final boolean optional) {
        String schemaName = schema.name();
        if (schemaName != null && schema.namespace() != null) {
            schemaName = schema.namespace() + "." + schemaName;
        }

        final SchemaBuilder sb = SchemaBuilder.struct()
                .name(schemaName)
                .doc(schema.doc());

        if (optional) {
            sb.optional();
        }

        for (final TypedField field : schema) {
            final String fieldName = field.name();
            final io.streamthoughts.kafka.connect.filepulse.data.Schema fieldSchema = field.schema();
            // Ignore schema NULL because cannot determine the expected type.
            if (fieldSchema.type() != Type.NULL && fieldSchema.isResolvable()) {
                String fieldSchemaName = normalizeSchemaName(fieldName);
                mayUpdateSchemaWithName(fieldSchema, fieldSchemaName);
                sb.field(fieldName, fieldSchema.map(this, true));
            } else {
                LOG.debug("Ignore field '{}', schema type is either NULL or cannot be resolved.", fieldName);
            }
        }
        return context.buildSchemaWithCyclicSchemaWrapper(sb.build());
    }

    private void mayUpdateSchemaWithName(final io.streamthoughts.kafka.connect.filepulse.data.Schema schema,
                                         final String schemaName) {
        if (schema.type() == Type.ARRAY) {
            final ArraySchema arraySchema = (ArraySchema) schema;
            mayUpdateSchemaWithName(arraySchema.valueSchema(), schemaName);
        }

        if (schema.type() == Type.MAP) {
            final MapSchema mapSchema = (MapSchema) schema;
            mayUpdateSchemaWithName(mapSchema.valueSchema(), schemaName);
        }

        if (schema.type() == Type.STRUCT) {
            final StructSchema structSchema = (StructSchema) schema;
            if (structSchema.name() == null) {
                structSchema.name(schemaName);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema map(final SimpleSchema schema, final boolean optional) {
        final SchemaBuilder builder = new SchemaBuilder(schema.type().schemaType());
        return (optional ? asNullableAndOptional(builder) : builder).build();
    }

    private static SchemaBuilder asNullableAndOptional(final SchemaBuilder sb) {
        return sb.optional().defaultValue(DEFAULT_NULL_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final MapSchema schema, final Map<String, ?> map, final boolean optional) {
        Schema connectSchema = schema.map(this, optional);
        return new SchemaAndValue(connectSchema, map);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final ArraySchema schema, final Collection<?> array, final boolean optional) {
        Schema connectSchema = schema.map(this, optional);

        return new SchemaAndValue(connectSchema, array);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final StructSchema schema, final TypedStruct struct, final boolean optional) {
        return map(schema.map(this, optional), struct);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final SimpleSchema schema, final Object value, final boolean optional) {
        return new SchemaAndValue(schema.map(this, optional), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue map(final Schema connectSchema, final TypedStruct value) {
        return new SchemaAndValue(connectSchema, toConnectStruct(connectSchema, value));
    }

    private static Struct toConnectStruct(final Schema connectSchema, final TypedStruct struct) {
        final Struct connectStruct = new Struct(connectSchema);

        for (Field connectField : connectSchema.fields()) {
            final String recordName = connectSchema.name();
            final String fieldName = connectField.name();

            final boolean isOptional = connectField.schema().isOptional();
            if (!struct.has(fieldName)) {
                if (!isOptional) {
                    throw new DataException(
                            "Failed to convert record to connect data. " +
                            "Missing required field '" + fieldName + "' for record '" + recordName + "'"
                    );
                }
                continue;
            }

            TypedValue typed = struct.get(fieldName);
            final Schema connectFieldSchema = connectField.schema();

            final Schema.Type dataSchemaType = typed.type().schemaType();
            final Schema.Type schemaType = connectFieldSchema.type();

            if (schemaType != dataSchemaType) {
                if (schemaType.equals(Schema.Type.ARRAY)) {
                    Schema.Type arrayValueType = connectFieldSchema.valueSchema().type();
                    if (!arrayValueType.equals(dataSchemaType)) {
                        throw new DataException(
                                "Failed to convert record field '" +
                                recordName + "." + fieldName + "' to connect data. " +
                                "Types do not match Array[" + arrayValueType + "]<>Array[" + dataSchemaType + "]"
                        );
                    }
                    typed = TypedValue.array(Collections.singleton(typed.value()), typed.schema());
                }
                // Handle specific STRING and NUMBER conversions for primitive type.
                else if (dataSchemaType.isPrimitive()) {
                    final boolean isNumber = typed.type().isNumber();
                    if (schemaType == Schema.Type.STRING) {
                        typed = typed.as(Type.STRING);
                        // handle INTEGER/LONG -> DOUBLE
                    } else if (schemaType == Schema.Type.FLOAT64 && isNumber) {
                        typed = typed.as(Type.DOUBLE);
                        // handle INTEGER -> LONG
                    } else if (schemaType == Schema.Type.INT64 && typed.type() == Type.INTEGER) {
                        typed = typed.as(Type.LONG);
                    }
                } else {
                    throw new DataException(
                            "Failed to convert record field '" +
                            recordName + "." + fieldName + "' to connect data. " +
                            "Types do not match " + schemaType + "<>" + dataSchemaType
                    );
                }
            }
            connectStruct.put(connectField, toConnectObject(connectFieldSchema, typed));
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
                                    e -> {
                                        TypedValue value = TypedValue.of(e.getKey(), keySchema);
                                        return toConnectObject(connectKeySchema, value);
                                    },
                                    e -> {
                                        Object converted = valueSchema.type().convert(e.getValue());
                                        TypedValue elemValue = TypedValue.of(converted, valueSchema);
                                        return toConnectObject(connectValueSchema, elemValue);
                                    }
                            )
                    );
        }

        if (schema.type() == Schema.Type.ARRAY) {
            final Schema connectValueSchema = schema.valueSchema();

            final io.streamthoughts.kafka.connect.filepulse.data.Schema valueSchema =
                    ((ArraySchema) typed.schema()).valueSchema();

            return typed.getArray()
                    .stream()
                    .map(value -> {
                        Object converted = valueSchema.type().convert(value);
                        TypedValue elemValue = TypedValue.of(converted, valueSchema);
                        return toConnectObject(connectValueSchema, elemValue);
                    })
                    .collect(Collectors.toList());
        }
        return typed.value();
    }
}
