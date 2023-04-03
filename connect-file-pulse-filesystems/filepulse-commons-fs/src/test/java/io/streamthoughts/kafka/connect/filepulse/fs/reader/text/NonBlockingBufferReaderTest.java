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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import static io.streamthoughts.kafka.connect.filepulse.fs.reader.text.NonBlockingBufferReader.DEFAULT_INITIAL_CAPACITY;

import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal.TextBlock;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NonBlockingBufferReaderTest {

    private static final String LF = "\n";
    private static final String CR = "\r";

    private static final int NLINES = 10;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;
    private BufferedWriter writer;


    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        writer = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset());
    }

    @Test
    public void shouldReadAllLinesGivenHigherInitialCapacityThanFileSize() throws Exception {
        final List<TextBlock> expected = generateLines(writer, NLINES, LF);
        NonBlockingBufferReader reader = createReaderWithCapacity(file, DEFAULT_INITIAL_CAPACITY);
        readAllAndAssert(expected, reader, false);
    }

    @Test
    public void shouldReadAllLinesGivenHigherInitialCapacityThanFileSizeAndCRLF() throws Exception {
        List<TextBlock> expected = generateLines(writer, NLINES, CR + LF);
        NonBlockingBufferReader reader = createReaderWithCapacity(file, DEFAULT_INITIAL_CAPACITY);
        readAllAndAssert(expected, reader, false);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanFileSize() throws Exception {
        List<TextBlock> expected = generateLines(writer, NLINES, LF);
        NonBlockingBufferReader reader = createReaderWithCapacity(file, 16);
        readAllAndAssert(expected, reader, false);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanFileSizeCRLF() throws Exception {
        List<TextBlock> expected = generateLines(writer, NLINES, CR + LF);
        NonBlockingBufferReader reader = createReaderWithCapacity(file, 16);
        readAllAndAssert(expected, reader, false);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanLineSize() throws Exception {
        List<TextBlock> expected = generateLines(writer, NLINES, LF);
        NonBlockingBufferReader reader = createReaderWithCapacity(file, 4);
        readAllAndAssert(expected, reader, false);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanLineSizeCRLF() throws Exception {
        List<TextBlock> expected = generateLines(writer, NLINES, CR + LF );
        NonBlockingBufferReader reader = createReaderWithCapacity(file, 4);
        readAllAndAssert(expected, reader, false);
    }

    @Test
    public void shouldReadAllLinesGivenFileNotEndingWithNewLine() throws Exception {
        List<TextBlock> expected = generateLines(writer, NLINES, CR + LF, false);
        NonBlockingBufferReader reader = createReaderWithCapacity(file, 1024);
        readAllAndAssert(expected, reader, false);
    }

    @Test
    public void shouldAttemptToReadMoreLinesThanMinimumGivenStrictEqualsFalse() throws Exception {
        generateLines(writer, NLINES, CR + LF, false);
        try(NonBlockingBufferReader reader = createReaderWithCapacity(file, DEFAULT_INITIAL_CAPACITY)) {
            List<TextBlock> records = reader.readLines(1, false);
            Assert.assertTrue(records.size() > 1);
        }
    }

    @Test
    public void shouldNotAttemptToReadMoreLinesThanMinimumGivenStrictEqualsTrue() throws Exception {
       generateLines(writer, NLINES, CR + LF, false);
        try(NonBlockingBufferReader reader = createReaderWithCapacity(file, DEFAULT_INITIAL_CAPACITY)) {
            List<TextBlock> records = reader.readLines(1, true);
            Assert.assertEquals(1, records.size());
        }
    }

    @Test
    public void shouldReadAllLinesGivenStrictEqualsTrue()throws Exception {
        final List<TextBlock> expected = generateLines(writer, NLINES, LF);
        NonBlockingBufferReader reader = createReaderWithCapacity(file, DEFAULT_INITIAL_CAPACITY);
        readAllAndAssert(expected, reader, true);
    }

    private static NonBlockingBufferReader createReaderWithCapacity(final File file,
                                                                    final int defaultInitialCapacity) throws FileNotFoundException {
        return new NonBlockingBufferReader(
                new FileInputStream(file),
                defaultInitialCapacity,
                Charset.defaultCharset());
    }

    private void readAllAndAssert(final List<TextBlock> expected,
                                  final NonBlockingBufferReader reader,
                                  final boolean strict) throws Exception {
        List<TextBlock> results = new ArrayList<>();
        while (reader.hasNext()) {
            List<TextBlock> l = reader.readLines(1, strict);
            results.addAll(l);
        }
        assertResult(expected, results);
        Assert.assertEquals(file.length(), reader.position());
        Assert.assertFalse(reader.remaining());
        reader.close();
    }

    private void assertResult(final List<TextBlock> expected, final List<TextBlock> results) {
        Assert.assertEquals(expected.size(), results.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i), results.get(i));
        }
    }

    private List<TextBlock> generateLines(final BufferedWriter writer,
                                          final int limit,
                                          final String newLine) throws IOException {
        return generateLines(writer, limit, newLine, true);
    }

    private List<TextBlock> generateLines(final BufferedWriter writer,
                                          final int limit,
                                          final String newLine,
                                          final boolean endWithNewLine) throws IOException {

        long offset = 0;
        List<TextBlock> generated = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            String line = "00000000-" + i;
            writer.write(line);
            if (i + 1 < limit || endWithNewLine) {
                writer.write(newLine);
                generated.add(new TextBlock(line, StandardCharsets.UTF_8, offset, offset + 10 + newLine.length(), 10));
            } else {
                generated.add(new TextBlock(line, StandardCharsets.UTF_8, offset, offset + 10, 10));
            }
            offset +=line.length() + newLine.length();
        }
        writer.flush();
        return generated;
    }
}