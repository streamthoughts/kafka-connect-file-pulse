/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.BaseAmazonS3Test;
import io.streamthoughts.kafka.connect.filepulse.fs.S3BucketKey;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AmazonS3ParquetInputReaderTest extends BaseAmazonS3Test {

    private static final String FILE_NAME = "src/test/resources/test.snappy.parquet";


    private File objectFile;

    private AmazonS3ParquetFileInputReader reader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        objectFile = new File(FILE_NAME);
        reader = new AmazonS3ParquetFileInputReader();
        reader.setStorage(new AmazonS3Storage(client));
        reader.configure(unmodifiableCommonsProperties);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
    }

    @Test
    public void should_read_all_lines() {
        client.createBucket(S3_TEST_BUCKET);
        client.putObject(S3_TEST_BUCKET, "my_key", objectFile);

        final GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(new S3BucketKey(S3_TEST_BUCKET, "my_key").toURI())
                .build();
        final FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(meta.uri());
        List<FileRecord<TypedStruct>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            final RecordsIterable<FileRecord<TypedStruct>> next = iterator.next();
            results.addAll(next.collect());
        }
        Assert.assertEquals(4, results.size());
    }

    @Test
    public void should_throw_reader_exception() {
        try (AmazonS3ParquetFileInputReader reader = mock(AmazonS3ParquetFileInputReader.class)) {
            when(reader.newIterator(any())).thenThrow(new ReaderException("exception"));

            assertThrows(ReaderException.class, () -> reader.newIterator(new URI("test")));
        }
    }
}