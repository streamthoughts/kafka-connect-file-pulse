/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.avro;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class BaseAvroDataIteratorTest {

    private static final long NOW = System.currentTimeMillis();

    private static final Schema DEFAULT_TEST_SCHEMA;
    private static final GenericData.Record DEFAULT_GENERIC_RECORD;

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
    private FileObjectMeta objectMeta;

    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        objectMeta = new LocalFileObjectMeta(file);
        writeGenericRecords(
                DEFAULT_TEST_SCHEMA,
                DEFAULT_GENERIC_RECORD,
                DEFAULT_GENERIC_RECORD,
                DEFAULT_GENERIC_RECORD
        );
    }

    @Test
    public void should_read_given_multiple_avro_record() {

        try (FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectMeta)) {
            Assert.assertTrue(iterator.hasNext());
            int records = 0;
            while (iterator.hasNext()) {
                iterator.next();
                records++;
            }
            Assert.assertEquals(3, records);
        }
    }

    @Test
    public void should_seek_to_given_valid_position() {
        final FileRecordOffset offset;
        try (FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectMeta)) {
            Assert.assertTrue(iterator.hasNext());
            RecordsIterable<FileRecord<TypedStruct>> next = iterator.next();
            offset = next.last().offset();
        }

        try (FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectMeta)) {
            iterator.seekTo(offset.toSourceOffset());
            // Attemps to read remaining records.
            Assert.assertTrue(iterator.hasNext());
            int records = 0;
            while (iterator.hasNext()) {
                iterator.next();
                records++;

            }
            Assert.assertEquals(2, records);
        }
    }

    public void writeGenericRecords(final Schema schema, final GenericRecord... records) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try {
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, file);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
            dataFileWriter.close();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    abstract FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileObjectMeta objectMeta);
}