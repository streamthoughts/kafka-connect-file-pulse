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
package io.streamthoughts.kafka.connect.filepulse.json;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.SchemaSupplier;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DefaultJSONStructConverter implements JSONStructConverter {

    private static final Map<ValueType, JsonFieldAccessor> ACCESSORS = new HashMap<>();

    /**
     * Creates a new {@link DefaultJSONStructConverter} instance.
     */
    DefaultJSONStructConverter() {
        ACCESSORS.put(ValueType.ARRAY, new ArrayJsonFieldAccessor());
        ACCESSORS.put(ValueType.STRING, new StringJsonFieldAccessor());
        ACCESSORS.put(ValueType.OBJECT, new ObjectJsonFieldAccessor());
        ACCESSORS.put(ValueType.BOOLEAN, new BooleanJsonFieldAccessor());
        ACCESSORS.put(ValueType.NUMBER, new NumberJsonFieldAccessor());
        ACCESSORS.put(ValueType.NULL, new NullJsonFieldAccessor());
    }

    private static JsonFieldAccessor getAccessorForType(final ValueType type) {
        if (type == ValueType.INVALID) {
            throw new ReaderException(
                "Error while reading value in JSON," +
                " invalid type encounter - this is generally due to an unexpected character.");
        }
        JsonFieldAccessor accessor = ACCESSORS.get(type);
        if (accessor == null) {
            throw new ReaderException("Error while reading value in JSON - Unknown type " + type);
        }
        return accessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue readJson(final String data) {

        if (data == null) return null;

        try {
            JsonIterator it = JsonIterator.parse(data);
            return getAccessorForType(it.whatIsNext()).read(it);

        } catch (Exception e) {
            throw new ReaderException("Error while reading json value, invalid JSON message.", e);
        }

    }

    private interface JsonFieldAccessor {

        TypedValue read(final JsonIterator it) throws IOException;
    }

    private static class NullJsonFieldAccessor implements JsonFieldAccessor {

        @Override
        public TypedValue read(JsonIterator it) throws IOException {
            it.readNull();
            return TypedValue.of(null, Schema.none());
        }
    }

    private static class NumberJsonFieldAccessor implements JsonFieldAccessor {

        @Override
        public TypedValue read(final JsonIterator it) throws IOException {
            Any any = it.readAny();

            Object object = any.object();
            if (object instanceof Long) {
                return TypedValue.int64(any.toLong());

            } else if (object instanceof Integer) {
                return TypedValue.int32(any.toInt());

            } else if (object instanceof Double) {
                return TypedValue.float64(any.toDouble());

            } else if (object instanceof Float) {
                return TypedValue.float32(any.toFloat());
            }

            throw new ReaderException("Unknown number type : " + object.getClass());
        }
    }

    private static class BooleanJsonFieldAccessor implements JsonFieldAccessor {

        @Override
        public TypedValue read(final JsonIterator it) throws IOException {
            return TypedValue.bool(it.readBoolean());
        }
    }

    private static class StringJsonFieldAccessor implements JsonFieldAccessor {

        @Override
        public TypedValue read(final JsonIterator it) throws IOException {
            return TypedValue.string(it.readString());
        }
    }
    private static class ArrayJsonFieldAccessor implements JsonFieldAccessor {


        @Override
        public TypedValue read(JsonIterator it) throws IOException {
            List<Object> array = new LinkedList<>();

            Type type = null;
            while (it.readArray()) {
                ValueType valueType = it.whatIsNext();
                TypedValue read = getAccessorForType(valueType).read(it);
                type = read.type();
                array.add(read.value());
            }
            return TypedValue.array(array, type != null ? Schema.of(type) : SchemaSupplier.lazy(array).get());
        }
    }
    private static class ObjectJsonFieldAccessor implements JsonFieldAccessor {

        @Override
        public TypedValue read(final JsonIterator it) throws IOException {

            TypedStruct struct = TypedStruct.create();

            for (String field = it.readObject(); field != null; field = it.readObject()) {
                ValueType valueType = it.whatIsNext();
                JsonFieldAccessor accessor = getAccessorForType(valueType);
                TypedValue value = accessor.read(it);
                struct.put(field, value);
            }
            return TypedValue.struct(struct);
        }
    }
}