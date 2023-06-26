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

import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_DIRECTORY_PATH;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_HOST;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_PASSWORD;
import static io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig.SFTP_LISTING_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFileStorage;
import io.streamthoughts.kafka.connect.filepulse.fs.iterator.SftpRowFileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SftpRowFileInputReaderTest {

    @Test
    void when_storage_not_initialized_configure_should_initialize_storage_and_factory() {
        SftpRowFileInputReader reader = spy(new SftpRowFileInputReader());
        SftpFileStorage storage = mock(SftpFileStorage.class);
        SftpRowFileInputIteratorFactory factory = mock(SftpRowFileInputIteratorFactory.class);

        doReturn(storage).when(reader).initStorage(eq(Fixture.config));
        doReturn(factory).when(reader).initIteratorFactory(eq(Fixture.config));

        reader.configure(Fixture.config);

        verify(reader).initStorage(eq(Fixture.config));
        verify(reader).initIteratorFactory(eq(Fixture.config));
    }

    @Test
    @SuppressWarnings("unchecked")
    void when_reader_configured_newIterator_should_initialize_initialize_the_iterator() {
        SftpRowFileInputReader reader = spy(new SftpRowFileInputReader());
        SftpFileStorage storage = mock(SftpFileStorage.class);
        SftpRowFileInputIteratorFactory factory = mock(SftpRowFileInputIteratorFactory.class);
        FileInputIterator<FileRecord<TypedStruct>> iterator = mock(FileInputIterator.class);
        when(factory.newIterator(any())).thenReturn(iterator);
        IteratorManager iteratorManager = mock(IteratorManager.class);

        doReturn(storage).when(reader).initStorage(eq(Fixture.config));
        doReturn(factory).when(reader).initIteratorFactory(eq(Fixture.config));

        reader.configure(Fixture.config);
        FileInputIterator<FileRecord<TypedStruct>> result = reader.newIterator(URI.create(""), iteratorManager);
        verify(factory).newIterator(any());
        assertThat(result).isEqualTo(iterator);
    }

    interface Fixture {
        Map<String, Object> config = Map.of(
                        SFTP_LISTING_HOST, "h",
                        SFTP_LISTING_USER, "u",
                        SFTP_LISTING_PASSWORD, "p",
                        SFTP_LISTING_DIRECTORY_PATH, "/path");
    }
}