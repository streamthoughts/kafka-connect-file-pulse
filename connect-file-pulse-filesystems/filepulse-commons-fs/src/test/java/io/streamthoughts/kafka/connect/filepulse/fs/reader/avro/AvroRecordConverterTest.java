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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.junit.Test;

public class AvroRecordConverterTest {

    private static final long NOW = System.currentTimeMillis();

    private static final Schema DEFAULT_TEST_SCHEMA;
    private static final GenericData.Record  DEFAULT_GENERIC_RECORD;

    static {
        DEFAULT_TEST_SCHEMA = SchemaBuilder.record("test")
                .fields()
                .requiredString("firstName")
                .requiredString("lastName")
                .requiredInt("age")
                .requiredLong("timestamp")
                .endRecord();

        DEFAULT_GENERIC_RECORD = new GenericData.Record(DEFAULT_TEST_SCHEMA);
        DEFAULT_GENERIC_RECORD.put("lastName", "foo");
        DEFAULT_GENERIC_RECORD.put("firstName", "bar");
        DEFAULT_GENERIC_RECORD.put("age", 42);
        DEFAULT_GENERIC_RECORD.put("timestamp", NOW);
    }

    @Test
    public void shouldConvertGenericRecordWithPrimitiveTypes() {

        final TypedStruct struct = AvroTypedStructConverter.fromGenericRecord(DEFAULT_GENERIC_RECORD);

        Assert.assertEquals("bar", struct.getString("firstName"));
        Assert.assertEquals("foo", struct.getString("lastName"));
        Assert.assertEquals(42, struct.getInt("age").intValue());
        Assert.assertEquals(NOW, struct.getLong("timestamp").longValue());
    }

    @Test
    public void shouldConvertGenericRecordWithArrayType() {
        final Schema schema = SchemaBuilder.record("test")
                .fields()
                .name("field")
                .type(SchemaBuilder.array().items(Schema.create(Schema.Type.STRING))).noDefault()
                .endRecord();

        GenericData.Record avro = new GenericData.Record(schema);
        avro.put("field", Arrays.asList("foo", "bar"));

        final TypedStruct struct = AvroTypedStructConverter.fromGenericRecord(avro);
        Assert.assertTrue(Arrays.asList("foo", "bar").containsAll(struct.getArray("field")));
    }

    @Test
    public void shouldConvertGenericRecordWithMapType() {
        final Schema schema = SchemaBuilder.record("test")
                .fields()
                .name("field")
                .type(SchemaBuilder.map().values(Schema.create(Schema.Type.STRING))).noDefault()
                .endRecord();

        Map<String, String> map = new HashMap<>();
        map.put("field1", "foo");
        map.put("field2", "bar");

        GenericData.Record avro = new GenericData.Record(schema);
        avro.put("field", map);

        final TypedStruct struct = AvroTypedStructConverter.fromGenericRecord(avro);

        Assert.assertEquals(map, struct.getMap("field"));
    }

    @Test
    public void shouldConvertGenericRecordWithUnionType() {
        final Schema schema = SchemaBuilder.record("test")
                .fields()
                .optionalString("field")
                .endRecord();

        GenericData.Record avro = new GenericData.Record(schema);
        avro.put("field", "string-value");

        final TypedStruct struct = AvroTypedStructConverter.fromGenericRecord(avro);

        Assert.assertEquals( "string-value", struct.getString("field"));
    }

    @Test
    public void shouldConvertGenericRecordWithRecordType() {
        final Schema schema = SchemaBuilder.record("test")
                .fields()
                .name("field")
                .type(DEFAULT_TEST_SCHEMA).noDefault()
                .endRecord();

        GenericData.Record avro = new GenericData.Record(schema);
        avro.put("field", DEFAULT_GENERIC_RECORD);

        final TypedStruct struct = AvroTypedStructConverter.fromGenericRecord(avro);

        TypedStruct field = struct.getStruct("field");

        Assert.assertEquals("bar", field.getString("firstName"));
        Assert.assertEquals("foo", field.getString("lastName"));
        Assert.assertEquals(42, field.getInt("age").intValue());
        Assert.assertEquals(NOW, field.getLong("timestamp").longValue());
    }
}