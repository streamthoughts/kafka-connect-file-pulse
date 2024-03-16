/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalRowFileInputReaderTest {

    private static final String LF = "\n";

    private static final int NLINES = 10;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File objectFile;

    private LocalRowFileInputReader reader;

    @Before
    public void setUp() throws Exception {
        objectFile = testFolder.newFile();
        BufferedWriter writer = Files.newBufferedWriter(objectFile.toPath(), Charset.defaultCharset());
        generateLines(writer);

        reader = new LocalRowFileInputReader();
        reader.configure(new HashMap<>());
    }

    public void tearDown() {
        reader.close();
    }

    @Test
    public void should_read_all_lines() {
        final GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(objectFile.toURI())
                .build();

        final FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(meta.uri());

        List<FileRecord<TypedStruct>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            final RecordsIterable<FileRecord<TypedStruct>> next = iterator.next();
            results.addAll(next.collect());
        }
        Assert.assertEquals(10, results.size());
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