/*
 * Copyright 2023 StreamThoughts.
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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.GcsStorage;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GcsRowFileInputReaderTest {

    private static final String LF = "\n";

    private static final String TEST_BUCKET_NAME = "test-bucket";
    private static final String TEST_BLOB_NAME = "test.csv";

    private static final int NLINES = 10;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private GcsRowFileInputReader reader;

    @Before
    public void setUp() throws Exception {
        final File objectFile = testFolder.newFile();
        try (BufferedWriter writer = Files.newBufferedWriter(objectFile.toPath(), Charset.defaultCharset())) {
            generateLines(writer);
        }

        Storage service = LocalStorageHelper.getOptions().getService();

        BlobInfo blobInfo = BlobInfo
                .newBuilder(BlobId.of(TEST_BUCKET_NAME, TEST_BLOB_NAME))
                .build();

        service.createFrom(blobInfo, objectFile.toPath());

        reader = new GcsRowFileInputReader();
        reader.setStorage(new GcsStorage(service));
        reader.configure(new HashMap<>());
    }

    @After
    public void tearDown() {
        reader.close();
    }

    @Test
    public void should_read_all_lines() {
        final GenericFileObjectMeta meta = new GenericFileObjectMeta.Builder()
                .withUri(GcsStorage.createBlobURI(TEST_BUCKET_NAME, TEST_BLOB_NAME))
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