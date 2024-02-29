/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class LocalParquetInputReaderTest {

    private static final String TEST_VALUE = "src/test/resources/datasets/test.snappy.parquet";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;
    private LocalParquetFileInputReader reader;

    @Before
    public void setUp() {
        file = new File(TEST_VALUE);

        reader = new LocalParquetFileInputReader();
        reader.configure(Collections.emptyMap());
    }

    @After
    public void tearDown() {
        reader.close();
    }

    @Test
    public void should_read_all_bytes() {
        FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(file.toURI());
        List<FileRecord<TypedStruct>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            final RecordsIterable<FileRecord<TypedStruct>> next = iterator.next();
            results.addAll(next.collect());
        }
        Assert.assertEquals(4, results.size());
    }
}