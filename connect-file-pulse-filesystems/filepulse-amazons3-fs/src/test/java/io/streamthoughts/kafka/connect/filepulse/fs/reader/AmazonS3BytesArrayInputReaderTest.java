/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.AmazonS3Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.BaseAmazonS3Test;
import io.streamthoughts.kafka.connect.filepulse.fs.S3BucketKey;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.File;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AmazonS3BytesArrayInputReaderTest extends BaseAmazonS3Test {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private byte[] content;

    private File objectFile;

    private AmazonS3BytesArrayInputReader reader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        objectFile = testFolder.newFile();
        try(OutputStream os = Files.newOutputStream(objectFile.toPath())) {
            content = getRandomByteArray();
            os.write(content);
            os.flush();
        }
        reader = new AmazonS3BytesArrayInputReader();
        reader.setStorage(new AmazonS3Storage(client));
        reader.configure(unmodifiableCommonsProperties);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
    }

    @Test
    public void should_read_all_bytes_given_s3_object() {
        client.createBucket(S3_TEST_BUCKET);
        client.putObject(S3_TEST_BUCKET, "my-key", objectFile);

        final GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(new S3BucketKey(S3_TEST_BUCKET, "my-key").toURI())
                .build();

        final FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(meta.uri());

        Assert.assertTrue(iterator.hasNext());
        final RecordsIterable<FileRecord<TypedStruct>> records = iterator.next();

        Assert.assertEquals(1, records.size());
        final byte[] actuals = records.last().value().get("message").value();
        Assert.assertArrayEquals(content, actuals);

    }

    private static byte[] getRandomByteArray() {
        int byteSize = 1024 * 4;
        final ByteBuffer bf = ByteBuffer.wrap(new byte[byteSize]);
        int bufferSize=1024;
        Random r = new Random();
        int nbBytes=0;
        while (nbBytes < byteSize){
            int nbBytesToWrite = Math.min(byteSize-nbBytes,bufferSize);
            byte[] bytes = new byte[nbBytesToWrite];
            r.nextBytes(bytes);
            bf.put(bytes);
            nbBytes+=nbBytesToWrite;
        }
        return bf.flip().array();
    }

}