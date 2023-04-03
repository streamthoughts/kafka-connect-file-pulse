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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReverseInputFileReaderTest {

    private static final String LF = "\n";
    private static final String CR = "\r";

    private static final int DEFAULT_NLINES = 10;

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
    public void shouldReturnIfAvailableRecordsIsLessThanMin() throws Exception {
        final List<TextBlock> expected = generateLines(writer, 2, LF);
        ReversedInputFileReader reader = createReaderWithCapacity(file, ReversedInputFileReader.DEFAULT_INITIAL_CAPACITY);
        readAllAndAssert(expected, reader, 10);
    }

    @Test
    public void shouldReadAllLinesGivenHigherInitialCapacityThanFileSize() throws Exception {
        final List<TextBlock> expected = generateLines(writer, DEFAULT_NLINES, LF);
        ReversedInputFileReader reader = createReaderWithCapacity(file, ReversedInputFileReader.DEFAULT_INITIAL_CAPACITY);
        readAllAndAssert(expected, reader, 1);
    }

    @Test
    public void shouldReadAllLinesGivenHigherInitialCapacityThanFileSizeAndCRLF() throws Exception {
        List<TextBlock> expected = generateLines(writer, DEFAULT_NLINES, CR + LF);
        ReversedInputFileReader reader = createReaderWithCapacity(file, ReversedInputFileReader.DEFAULT_INITIAL_CAPACITY);
        readAllAndAssert(expected, reader, 1);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanFileSize() throws Exception {
        List<TextBlock> expected = generateLines(writer, DEFAULT_NLINES, LF);
        ReversedInputFileReader reader = createReaderWithCapacity(file, 16);
        readAllAndAssert(expected, reader, 1);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanFileSizeCRLF() throws Exception {
        List<TextBlock> expected = generateLines(writer, DEFAULT_NLINES, CR + LF);
        ReversedInputFileReader reader = createReaderWithCapacity(file, 16);
        readAllAndAssert(expected, reader, 1);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanLineSize() throws Exception {
        List<TextBlock> expected = generateLines(writer, DEFAULT_NLINES, LF);
        ReversedInputFileReader reader = createReaderWithCapacity(file, 4);
        readAllAndAssert(expected, reader, 1);
    }

    @Test
    public void shouldReadAllLinesGivenSmallerInitialCapacityThanLineSizeCRLF() throws Exception {
        List<TextBlock> expected = generateLines(writer, DEFAULT_NLINES, CR + LF );
        ReversedInputFileReader reader = createReaderWithCapacity(file, 4);
        readAllAndAssert(expected, reader, 1);
    }

    private ReversedInputFileReader createReaderWithCapacity(final File file,
                                                             final int defaultInitialCapacity) throws IOException {
        return new ReversedInputFileReader(
                file.getAbsolutePath(),
                defaultInitialCapacity,
                Charset.defaultCharset());
    }

    private void readAllAndAssert(final List<TextBlock> expected,
                                  final ReversedInputFileReader reader,
                                  final int minRecords) throws Exception {
        List<TextBlock> results = new ArrayList<>();
        while (reader.hasNext()) {
            List<TextBlock> l = reader.readLines(minRecords);
            results.addAll(l);
        }
        assertResult(expected, results);
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

        long offset = 0;
        List<TextBlock> generated = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            String line = "00000000-" + i;
            writer.write(line);
            writer.write(newLine);
            generated.add(new TextBlock(line, StandardCharsets.UTF_8, offset, offset + 10, 10));
            offset +=line.length() + newLine.length();
        }
        writer.flush();
        Collections.reverse(generated);
        return generated;
    }
}