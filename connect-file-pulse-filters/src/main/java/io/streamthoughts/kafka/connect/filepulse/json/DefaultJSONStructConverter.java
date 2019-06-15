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
package io.streamthoughts.kafka.connect.filepulse.json;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DefaultJSONStructConverter implements JSONStructConverter {

    private static final Map<ValueType, JsonFieldAccessor<?>> ACCESSORS = new HashMap<>();
    private static final ObjectJsonFieldAccessor DEFAULT_ACCESSOR = new ObjectJsonFieldAccessor();

    /**
     * Creates a new {@link DefaultJSONStructConverter} instance.
     */
    public DefaultJSONStructConverter() {
        ACCESSORS.put(ValueType.ARRAY, new ArrayJsonFieldAccessor());
        ACCESSORS.put(ValueType.STRING, new StringJsonFieldAccessor());
        ACCESSORS.put(ValueType.OBJECT, DEFAULT_ACCESSOR);
        ACCESSORS.put(ValueType.BOOLEAN, new BooleanJsonFieldAccessor());
        ACCESSORS.put(ValueType.NUMBER, new NumberJsonFieldAccessor());
    }

    private static JsonFieldAccessor<?> getAccessorForType(final ValueType type) {
        if (type == ValueType.INVALID) {
            throw new ReaderException(
                "Error while reading data in JSON," +
                " invalid type encounter - this is generally due to an unexpected character.");
        }
        JsonFieldAccessor<?> accessor = ACCESSORS.get(type);
        if (accessor == null) {
            throw new ReaderException("Error while reading data in JSON - Unknown type " + type);
        }
        return accessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputData readJson(final String data) {

        if (data == null) {
            return null;
        }

        try {
            JsonIterator it = JsonIterator.parse(data);
            // TODO should cache schema for a same context
            Schema schema = DEFAULT_ACCESSOR.getSchema(it, null);

            it = JsonIterator.parse(data);
            final Struct struct = DEFAULT_ACCESSOR.getStruct(it, schema);
            return new FileInputData(struct);

        } catch (Exception e) {
            throw new ReaderException("Error while reading json data, invalid JSON withMessage.", e);
        }

    }

    private interface JsonFieldAccessor<T> {

        Schema getSchema(final JsonIterator it, final String name) throws Exception;

        T getStruct(final JsonIterator it, final Schema schema) throws IOException;
    }

    private static class NumberJsonFieldAccessor implements JsonFieldAccessor<Object> {

        @Override
        public Schema getSchema(final JsonIterator it, final String name) throws Exception {
            Any any = it.readAny();

            SchemaBuilder builder;
            Object object = any.object();
            if (object instanceof Long) {
                builder = SchemaBuilder.int64();
            } else if (object instanceof Integer) {
                builder = SchemaBuilder.int32();
            } else if (object instanceof Double) {
                builder = SchemaBuilder.float64();
            } else if (object instanceof Float) {
                builder = SchemaBuilder.float32();
            } else {
                throw new ReaderException("Unknown number type : " + object.getClass());
            }
            return builder.name(name).optional().build();
        }

        @Override
        public Object getStruct(final JsonIterator it, final Schema schema) throws IOException {
            Any any = it.readAny();
            Schema.Type type = schema.type();
            if (type.equals(Schema.INT64_SCHEMA.type())) {
                return any.toLong();
            }
            if (type.equals(Schema.INT32_SCHEMA.type())) {
                return any.toInt();
            }
            if (type.equals(Schema.FLOAT32_SCHEMA.type())) {
                return any.toFloat();
            }
            if (type.equals(Schema.FLOAT64_SCHEMA.type())) {
                return any.toDouble();
            }
            throw new ReaderException("Unknown number type : " + type.getClass());
        }
    }

    private static class BooleanJsonFieldAccessor implements JsonFieldAccessor<Boolean> {

        @Override
        public Schema getSchema(final JsonIterator it,
                                final String name) throws Exception {
            it.skip();
            return SchemaBuilder.bool().name(name).optional().build();
        }

        @Override
        public Boolean getStruct(final JsonIterator it, final Schema schema) throws IOException {
            return it.readBoolean();
        }
    }

    private static class StringJsonFieldAccessor implements JsonFieldAccessor<String> {

        @Override
        public Schema getSchema(final JsonIterator it,
                                final String name) throws Exception {
            it.skip();
            return SchemaBuilder.string().name(name).optional().build();
        }

        @Override
        public String getStruct(final JsonIterator it, final Schema schema) throws IOException {
            return it.readString();
        }
    }
    private static class ArrayJsonFieldAccessor implements JsonFieldAccessor<List<?>> {

        @Override
        public Schema getSchema(final JsonIterator it,
                                final String name) throws Exception {

            Schema schema = null;
            while (it.readArray()) {
                ValueType valueType = it.whatIsNext();
                schema = getAccessorForType(valueType).getSchema(it, null);
            }
            return SchemaBuilder.array(schema).optional().build();
        }

        @Override
        public List<?> getStruct(JsonIterator it, final Schema schema) throws IOException {
            List<Object> array = new LinkedList<>();
            while (it.readArray()) {
                ValueType valueType = it.whatIsNext();
                array.add(getAccessorForType(valueType).getStruct(it, schema.valueSchema()));
            }
            return array;
        }
    }
    private static class ObjectJsonFieldAccessor implements JsonFieldAccessor<Struct> {

        @Override
        public Schema getSchema(final JsonIterator it,
                                final String name) throws Exception {

            SchemaBuilder builder = SchemaBuilder.struct().optional();
            for (String field = it.readObject(); field != null; field = it.readObject()) {
                ValueType valueType = it.whatIsNext();
                JsonFieldAccessor<?> accessor = getAccessorForType(valueType);
                builder.field(field, accessor.getSchema(it, name));
            }
            return builder.optional().build();
        }

        @Override
        public Struct  getStruct(final JsonIterator it,
                                 final Schema schema) throws IOException {

            Struct struct = new Struct(schema);
            for (String field = it.readObject(); field != null; field = it.readObject()) {
                ValueType valueType = it.whatIsNext();
                JsonFieldAccessor<?> accessor = getAccessorForType(valueType);
                Object object = accessor.getStruct(it, schema.field(field).schema());
                struct.put(field, object);
            }
            return struct;
        }
    }
}