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
package io.streamthoughts.kafka.connect.filepulse.source;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class FileInputData implements Iterable<Field> {

    public static final String DEFAULT_MESSAGE_FIELD = "message";

    public static SchemaBuilder defaultSchema() {
        return SchemaBuilder.struct()
                .field(DEFAULT_MESSAGE_FIELD, SchemaBuilder.string().optional());
    }

    private final Struct value;

    public static FileInputData defaultStruct(final String value) {
        return new FileInputData(new Struct(defaultSchema().build()).put(DEFAULT_MESSAGE_FIELD, value));
    }

    /**
     * Creates a new {@link FileInputData} instance.
     *
     * @param value   the data struct.
     */
    public FileInputData(final Struct value) {
        Objects.requireNonNull(value, "struct can't be null");
        this.value = value;
    }

    public Object get(final String fieldName) {
        return value.get(fieldName);
    }

    public Object get(final Field field) {
        return value.get(field);
    }


    public Field field(final String fieldName) {
        return schema().field(fieldName);
    }

    public boolean has(final String fieldName) {
        return schema().field(fieldName) != null;
    }

    public Struct value() {
        return value;
    }

    public Schema schema() {
        return value.schema();
    }

    public SchemaAndValue toSchemaAndValue() {
        return new SchemaAndValue(schema(), value);
    }

    @SuppressWarnings("unchecked")
    public <T> T getFirstValueForField(final String fieldName) {
        final Field field = value.schema().field(fieldName);
        if (field == null) {
            throw new RuntimeException("No field name : " + fieldName);
        }

        if (field.schema().type().equals(Schema.Type.ARRAY)) {
            List<T> array = value.getArray(fieldName);
            if (array.isEmpty()) return null;

            return array.get(0);
        }

        return (T) value.get(fieldName);
    }


    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public Iterator<Field> iterator() {
        return schema().fields().iterator();
    }
}
