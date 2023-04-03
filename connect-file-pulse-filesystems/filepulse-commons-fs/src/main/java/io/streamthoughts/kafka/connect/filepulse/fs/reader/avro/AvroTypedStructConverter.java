/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.avro;

import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.bool;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.float32;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.float64;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.int32;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.int64;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.string;
import static io.streamthoughts.kafka.connect.filepulse.data.TypedValue.struct;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * The {@link AvroTypedStructConverter} can be used to convert an avro object into a {@link TypedStruct}.
 */
public class AvroTypedStructConverter {

    private static final Map<Schema.Type, BiFunction<Schema, Object, TypedValue>> AVRO_TYPES_TO_CONVERTER;

    static {
        AVRO_TYPES_TO_CONVERTER = new HashMap<>();
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.BYTES, AvroTypedStructConverter::convertBytes);
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.ENUM, AvroTypedStructConverter::convertEnum);
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.STRING, AvroTypedStructConverter::convertString);
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.UNION, AvroTypedStructConverter::convertUnion);
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.ARRAY, AvroTypedStructConverter::convertCollection);
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.MAP, AvroTypedStructConverter::convertMap);
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.BOOLEAN,
                (schema, value) -> bool((Boolean) value));
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.INT,
                (schema, value) -> int32((Integer) value));
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.LONG,
                (schema, value) -> int64((Long) value));
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.FLOAT,
                (schema, value) -> float32((Float) value));
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.DOUBLE,
                (schema, value) -> float64((Double) value));
        AVRO_TYPES_TO_CONVERTER.put(Schema.Type.RECORD,
                (schema, value) -> struct(fromGenericRecord((GenericRecord) value)));
    }

    /**
     * Converts the specified {@link GenericRecord} instance into a {@link TypedStruct}.
     *
     * @param record the {@link GenericRecord} instance to convert.
     * @return a new {@link TypedStruct} instance.
     */
    static TypedStruct fromGenericRecord(final GenericRecord record) {
        TypedStruct struct = TypedStruct.create();
        final Schema schema = record.getSchema();
        for (Schema.Field field : schema.getFields()) {
            final String name = field.name();
            final Object value = record.get(name);
            struct = struct.put(name, fromSchemaAndValue(field.schema(), value));
        }
        return struct;
    }

    /**
     * converts the specified object into {@link TypedValue}.
     *
     * @param schema the {@link Schema} instance.
     * @param value  the record value.
     * @return a new {@link TypedValue} instance.
     */
    private static TypedValue fromSchemaAndValue(final Schema schema, final Object value) {
        final Schema.Type fieldType = schema.getType();

        BiFunction<Schema, Object, TypedValue> converter = AVRO_TYPES_TO_CONVERTER.get(fieldType);
        if (converter == null) {
            throw new ReaderException("Unsupported avro type : " + fieldType);
        }
        return converter.apply(schema, value);
    }


    private static TypedValue convertEnum(final Schema schema,
                                          final Object value) {
        final String stringValue = (value != null) ? ((Enum) value).name() : null;
        return string(stringValue);
    }

    private static TypedValue convertUnion(final Schema schema,
                                           final Object value) {
        final List<Schema> types = schema.getTypes();
        final Optional<Schema> nonNullSchema = types
                .stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst();

        if (nonNullSchema.isEmpty()) {
            throw new ReaderException("Unsupported avro type. Union should contain at-least one non-null type.");
        }
        return fromSchemaAndValue(nonNullSchema.get(), value);
    }

    private static TypedValue convertString(final Schema schema,
                                            final Object value) {
        // use org.apache.avro.util.Utf8 for string value.
        final String stringValue = (value != null) ? value.toString() : null;
        return string(stringValue);
    }

    private static TypedValue convertBytes(final Schema schema,
                                           final Object value) {
        return (value != null) ?
                TypedValue.any(value).as(Type.BYTES) :
                TypedValue.of(null, Type.BYTES);
    }

    @SuppressWarnings("unchecked")
    private static TypedValue convertMap(final Schema schema,
                                         final Object value) {

        Map<Object, Object> map = (Map<Object, Object>) value;
        final Schema valueSchema = schema.getValueType();
        Type mapValueType = null;
        final Map<String, Object> converted = new HashMap<>();
        for (Map.Entry<Object, Object> o : map.entrySet()) {
            TypedValue element = fromSchemaAndValue(valueSchema, o.getValue());
            // use org.apache.avro.util.Utf8 for string value.
            converted.put(o.getKey().toString(), element.value());
            mapValueType = element.type();
        }
        return (mapValueType != null) ?
                TypedValue.map(converted, mapValueType) :
                TypedValue.of(converted, Type.MAP);
    }

    @SuppressWarnings("unchecked")
    private static TypedValue convertCollection(final Schema schema,
                                                final Object value) {
        final Collection<Object> array = (Collection<Object>) value;
        final Schema elementSchema = schema.getElementType();
        Type arrayType = null;
        final Collection<Object> converted = new ArrayList<>(array.size());
        for (Object o : array) {
            TypedValue element = fromSchemaAndValue(elementSchema, o);
            converted.add(element.value());
            arrayType = element.type();
        }
        return (arrayType != null) ?
                TypedValue.array(converted, arrayType) :
                TypedValue.of(converted, Type.ARRAY);
    }
}
