/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BytesArrayInputReaderTest {

    private static final String TEST_VALUE = "test values\ntest values\ntest values";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;
    private LocalBytesArrayInputReader reader;

    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        try (BufferedWriter bw = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset())) {
             bw.append(TEST_VALUE);
             bw.flush();
         }

        reader = new LocalBytesArrayInputReader();
        reader.configure(Collections.emptyMap());
    }

    @After
    public void tearDown() {
        reader.close();
    }

    @Test
    public void should_readd_all_bytes() {
        FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(file.toURI());
        Assert.assertTrue(iterator.hasNext());

        FileRecord<TypedStruct> record = iterator.next().last();
        TypedStruct typed = record.value();

        TypedValue typedValue = typed.get(TypedFileRecord.DEFAULT_MESSAGE_FIELD);
        Assert.assertEquals(Type.BYTES, typedValue.type());
        Assert.assertTrue( Arrays.equals(TEST_VALUE.getBytes(), typedValue.getBytes()));
        Assert.assertFalse(iterator.hasNext());
    }
}