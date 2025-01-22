/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.BaseGzipAmazonS3Test;
import io.streamthoughts.kafka.connect.filepulse.fs.S3BucketKey;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AmazonS3RowGzipFileInputReaderTest extends BaseGzipAmazonS3Test {

    private static final String LF = "\n";

    private static final int NLINES = 10;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File objectFile;

    private AmazonS3RowFileInputReader reader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        objectFile = testFolder.newFile();
        System.out.println(objectFile.toPath());
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(objectFile.toPath().toString())), StandardCharsets.UTF_8));
        generateLines(writer);

        reader = new AmazonS3RowFileInputReader();
        reader.setStorage(new AmazonS3Storage(client));
        reader.configure(unmodifiableCommonsProperties);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
    }

    @Test
    public void should_read_all_gzip_lines() {
        client.createBucket(S3_TEST_BUCKET);
        client.putObject(S3_TEST_BUCKET, "my-key", objectFile);

        final GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(new S3BucketKey(S3_TEST_BUCKET, "my-key").toURI())
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
        writer.close();
    }
}