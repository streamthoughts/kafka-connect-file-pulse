/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RowFileInputIteratorTest {

    private static final String LF = "\n";

    private static final int NLINES = 10;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;

    private RowFileInputIterator iterator;


    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        try(BufferedWriter writer = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset())) {
            generateLines(writer);
        }
        final FileInputStream stream = new FileInputStream(file);
        iterator = new RowFileInputIterator(
            new LocalFileObjectMeta(file),
            new IteratorManager(),
            new NonBlockingBufferReader(stream));
    }

    @After
    public void tearDown() {
        iterator.close();
    }

    @Test
    public void test() {
        iterator.seekTo(new FileObjectOffset(0, 0, System.currentTimeMillis()));
        while(iterator.hasNext()) {
            RecordsIterable<FileRecord<TypedStruct>> next = iterator.next();
            Assert.assertNotNull(next);
        }

        FileObjectContext context = iterator.context();
        Assert.assertEquals(NLINES, context.offset().rows());
        Assert.assertEquals(file.length(), context.offset().position());
    }

    private void generateLines(final BufferedWriter writer) throws IOException {

        for (int i = 0; i < NLINES; i++) {
            String line = "00000000-" + i;
            writer.write(line);
            if (i + 1 < NLINES) {
                writer.write(LF);
            }
        }
        writer.flush();
    }
}