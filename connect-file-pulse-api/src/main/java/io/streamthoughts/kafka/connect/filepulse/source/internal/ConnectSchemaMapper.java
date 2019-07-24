/*
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
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectSchemaMapper implements SchemaMapper<Schema>, SchemaMapperWithValue<SchemaAndValue> {

    private static final Object DEFAULT_NULL_VALUE = null;

    public static final ConnectSchemaMapper INSTANCE = new ConnectSchemaMapper();

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
        for(final TypedField field : schema) {
            sb.field(field.name(), field.schema().map(this)).optional();
        }
        return sb.build();
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
            final TypedValue typed = struct.get(field.name());
            final Schema fieldSchema = field.schema();

            if (fieldSchema.type() != typed.type().schemaType()) {
                throw new DataException("types do not match " + fieldSchema.type() + "<>" + typed.type());
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
