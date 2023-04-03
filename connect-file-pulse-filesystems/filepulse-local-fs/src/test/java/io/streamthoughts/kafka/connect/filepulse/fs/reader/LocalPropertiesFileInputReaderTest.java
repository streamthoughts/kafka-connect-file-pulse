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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LocalPropertiesFileInputReaderTest {

    private static final URI FILE_URI;

    static {
        try {
            var cl = LocalPropertiesFileInputReaderTest.class.getClassLoader();
            FILE_URI = Objects.requireNonNull(cl.getResource("./datasets/test-LocalPropertiesFileInputReader.properties")).toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void should_load_record_given_input_properties_file() {
        LocalPropertiesFileInputReader reader = new LocalPropertiesFileInputReader();
        reader.configure(Collections.emptyMap());
        FileInputIterator<FileRecord<TypedStruct>> iterator = reader.newIterator(FILE_URI, new IteratorManager());

        RecordsIterable<FileRecord<TypedStruct>> result = iterator.next();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        FileRecord<TypedStruct> record = result.last();
        TypedStruct value = record.value();
        Assertions.assertEquals("value1", value.find("property.path.1").getString());
        Assertions.assertEquals("value2", value.find("property.path.2").getString());
        Assertions.assertEquals("value3", value.find("property.path.3").getString());
    }
}