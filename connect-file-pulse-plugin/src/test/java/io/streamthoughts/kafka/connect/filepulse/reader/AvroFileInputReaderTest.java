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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroFileInputReaderTest {

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

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;
    private FileContext context;

    private AvroFileInputReader reader;

    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        context = new FileContext(SourceMetadata.fromFile(file));
        reader = new AvroFileInputReader();
    }

    @After
    public void tearDown() {
        reader.close();
    }

    @Test
    public void shouldConvertGenericRecordWithPrimitiveTypes() {

        final TypedStruct struct = AvroFileInputReader.TypedStructConverter.fromGenericRecord(DEFAULT_GENERIC_RECORD);

        assertEquals("bar", struct.getString("firstName"));
        assertEquals("foo", struct.getString("lastName"));
        assertEquals(42, struct.getInt("age").intValue());
        assertEquals(NOW, struct.getLong("timestamp").longValue());
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

        final TypedStruct struct = AvroFileInputReader.TypedStructConverter.fromGenericRecord(avro);
        assertTrue(Arrays.asList("foo", "bar").containsAll(struct.getArray("field")));
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

        final TypedStruct struct = AvroFileInputReader.TypedStructConverter.fromGenericRecord(avro);

        assertEquals(map, struct.getMap("field"));
    }

    @Test
    public void shouldConvertGenericRecordWithUnionType() {
        final Schema schema = SchemaBuilder.record("test")
                .fields()
                .optionalString("field")
                .endRecord();

        GenericData.Record avro = new GenericData.Record(schema);
        avro.put("field", "string-value");

        final TypedStruct struct = AvroFileInputReader.TypedStructConverter.fromGenericRecord(avro);

        assertEquals( "string-value", struct.getString("field"));
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

        final TypedStruct struct = AvroFileInputReader.TypedStructConverter.fromGenericRecord(avro);

        TypedStruct field = struct.getStruct("field");

        assertEquals("bar", field.getString("firstName"));
        assertEquals("foo", field.getString("lastName"));
        assertEquals(42, field.getInt("age").intValue());
        assertEquals(NOW, field.getLong("timestamp").longValue());
    }

    @Test
    public void shouldReadGivenMultipleAvroRecord() {

        writeGenericRecords(
            DEFAULT_TEST_SCHEMA,
            DEFAULT_GENERIC_RECORD,
            DEFAULT_GENERIC_RECORD,
            DEFAULT_GENERIC_RECORD);

        FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(context);

        assertTrue(iterator.hasNext());
        int records = 0;
        while (iterator.hasNext()) {
            iterator.next();
            records++;
        }
        assertEquals(3, records);
    }

    @Test
    public void shouldSeekToGivenAValidPosition() {

        writeGenericRecords(
                DEFAULT_TEST_SCHEMA,
                DEFAULT_GENERIC_RECORD,
                DEFAULT_GENERIC_RECORD,
                DEFAULT_GENERIC_RECORD);

        FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(context);
        assertTrue(iterator.hasNext());

        RecordsIterable<FileRecord<TypedStruct>> next = iterator.next();
        final FileRecordOffset offset = next.last().offset();

        // Close the first iterator.
        iterator.close();

        // Get a new iterator.
        iterator = reader.newIterator(context);
        iterator.seekTo(offset.toSourceOffset());

        // Attemps to read remaining records.
        assertTrue(iterator.hasNext());
        int records = 0;
        while (iterator.hasNext()) {
            iterator.next();
            records++;

        }
        assertEquals(2, records);
    }

    public void writeGenericRecords(final Schema schema, final GenericRecord...records) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try {
            DataFileWriter <GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, file);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
            dataFileWriter.close();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
}