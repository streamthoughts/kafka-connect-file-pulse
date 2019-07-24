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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class RowFileInputIteratorTest {

    private static final String LF = "\n";

    private static final int NLINES = 10;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;
    private BufferedWriter writer;

    private FileContext context;


    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        writer = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset());
        generateLines(writer);
        context = new FileContext(SourceMetadata.fromFile(file));

    }

    @Test
    public void test() {
        RowFileInputIterator iterator = RowFileInputIterator.newBuilder()
                .withContext(context)
                .withIteratorManager(new IteratorManager())
                .build();
        iterator.seekTo(new SourceOffset(0, 0, System.currentTimeMillis()));
        while(iterator.hasNext()) {
            RecordsIterable<FileRecord<TypedStruct>> next = iterator.next();
            assertNotNull(next);

        }

        FileContext context = iterator.context();
        assertEquals(file.length(), context.offset().position());
        assertEquals(NLINES, context.offset().rows());
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