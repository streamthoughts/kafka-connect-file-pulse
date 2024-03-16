/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.internal.Silent;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import org.junit.Before;
import org.junit.Test;

public class ParquetFileInputIteratorTest {
    private static final String FILE_NAME = "src/test/resources/test.snappy.parquet";
    private FileObjectMeta objectMeta;
    @Before
    public void setUp() throws IOException, URISyntaxException {
        File file = new File(FILE_NAME);
        objectMeta = new LocalFileObjectMeta(file);
    }

    @Test
    public void should_read_given_multiple_parquet_record() {

        try (FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectMeta)) {
            assertTrue(iterator.hasNext());

            while (iterator.hasNext()) {
                iterator.next();
            }
            assertEquals(4, iterator.context().offset().position());
            assertEquals(0, iterator.context().offset().rows());
        }
    }

    @Test
    public void should_seek_to_given_valid_position() {
        long offset;
        try (FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectMeta)) {
            assertTrue(iterator.hasNext());
            iterator.next();
            offset = iterator.context().offset().position();
        }

        try (FileInputIterator<FileRecord<TypedStruct>> iterator = newIterator(objectMeta)) {
            iterator.seekTo(new FileObjectOffset(offset, 0, System.currentTimeMillis()));
            int record = 0;
            while (iterator.hasNext()) {
                iterator.next();
                record++;
            }
            assertEquals(4, iterator.context().offset().position());
            assertEquals(2, record);

        }
    }


    private FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileObjectMeta objectMeta) {
        return Silent.unchecked(() -> new ParquetFileInputIterator(
                objectMeta,
                new IteratorManager(),
                new FileInputStream(new File(objectMeta.uri())))
        );
    }
}