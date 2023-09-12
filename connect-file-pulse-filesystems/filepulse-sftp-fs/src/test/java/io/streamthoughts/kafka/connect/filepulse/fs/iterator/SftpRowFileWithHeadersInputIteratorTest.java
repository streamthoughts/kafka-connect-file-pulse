/*
 * Copyright 2023 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.NonBlockingBufferReader;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SftpRowFileWithHeadersInputIteratorTest {

    @Test
    @SneakyThrows
    void when_skipHeaders_is_zero_initHeaders_shouldThrow_exception() {
        URI fUri = URI.create("data/test_data.csv");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(fUri.toString());

        FileObjectMeta meta = mock(FileObjectMeta.class);
        when(meta.uri()).thenReturn(fUri);

        FileObjectContext context = new FileObjectContext(meta);

        FileInputIterator<FileRecord<TypedStruct>> iterator = mock(FileInputIterator.class);
        when(iterator.context()).thenReturn(context);

        NonBlockingBufferReader reader = spy(new NonBlockingBufferReader(raw));

        SftpRowFileWithHeadersInputIterator rowFileWithHeadersInputIterator =
                new SftpRowFileWithHeadersInputIterator(0, reader, iterator);

        assertThatThrownBy(rowFileWithHeadersInputIterator::initHeaders)
                .isInstanceOf(IllegalArgumentException.class)
                .hasNoCause();
    }

    @Test
    @SneakyThrows
    void when_inputstream_is_closed_initHeaders_shouldThrow_exception() {
        URI fUri = URI.create("data/test_data.csv");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(fUri.toString());

        FileObjectMeta meta = mock(FileObjectMeta.class);
        when(meta.uri()).thenReturn(fUri);

        FileObjectContext context = new FileObjectContext(meta);

        FileInputIterator<FileRecord<TypedStruct>> iterator = mock(FileInputIterator.class);
        when(iterator.context()).thenReturn(context);

        NonBlockingBufferReader reader = spy(new NonBlockingBufferReader(raw));
        raw.close();

        SftpRowFileWithHeadersInputIterator rowFileWithHeadersInputIterator =
                new SftpRowFileWithHeadersInputIterator(1, reader, iterator);

        assertThatThrownBy(rowFileWithHeadersInputIterator::initHeaders)
                .isInstanceOf(ReaderException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("Cannot read lines from");
    }

    @Test
    @SneakyThrows
    void when_inputfile_isEmpty_initHeaders_shouldThrow_exception() {
        URI fUri = URI.create("data/empty.csv");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(fUri.toString());

        FileObjectMeta meta = mock(FileObjectMeta.class);
        when(meta.uri()).thenReturn(fUri);

        FileObjectContext context = new FileObjectContext(meta);

        FileInputIterator<FileRecord<TypedStruct>> iterator = mock(FileInputIterator.class);
        when(iterator.context()).thenReturn(context);

        NonBlockingBufferReader reader = spy(new NonBlockingBufferReader(raw));

        SftpRowFileWithHeadersInputIterator rowFileWithHeadersInputIterator =
                new SftpRowFileWithHeadersInputIterator(1, reader, iterator);

        assertThatThrownBy(rowFileWithHeadersInputIterator::initHeaders)
                .isInstanceOf(ReaderException.class)
                .hasMessageContaining("Not enough data for reading")
                .hasNoCause();
    }

    @Test
    @SneakyThrows
    void when_valid_csv_with_headers_initHeaders_should_extract_header_names() {
        URI fUri = URI.create("data/test_data.csv");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(fUri.toString());

        FileObjectMeta meta = mock(FileObjectMeta.class);
        when(meta.uri()).thenReturn(fUri);

        FileObjectContext context = new FileObjectContext(meta);

        FileInputIterator<FileRecord<TypedStruct>> iterator = mock(FileInputIterator.class);
        when(iterator.context()).thenReturn(context);

        NonBlockingBufferReader reader = spy(new NonBlockingBufferReader(raw));

        SftpRowFileWithHeadersInputIterator rowFileWithHeadersInputIterator =
                new SftpRowFileWithHeadersInputIterator(1, reader, iterator);

        rowFileWithHeadersInputIterator.initHeaders();

        assertThat(rowFileWithHeadersInputIterator.getHeaderNames())
                .containsExactlyInAnyOrder(Fixture.expectedHeadersBlock);
    }

    @Test
    @SneakyThrows
    @SuppressWarnings("unchecked")
    void when_valid_csv_next_should_return_row_data() {
        URI fUri = URI.create("data/test_data.csv");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(fUri.toString());

        FileObjectMeta meta = mock(FileObjectMeta.class);
        when(meta.uri()).thenReturn(fUri);

        FileObjectContext context = new FileObjectContext(meta);

        FileInputIterator<FileRecord<TypedStruct>> iterator = mock(FileInputIterator.class);
        RecordsIterable<FileRecord<TypedStruct>> records = mock(RecordsIterable.class);
        doReturn(context).when(iterator).context();
        doReturn(records).when(iterator).next();

        NonBlockingBufferReader reader = spy(new NonBlockingBufferReader(raw));

        SftpRowFileWithHeadersInputIterator rowFileWithHeadersInputIterator =
                new SftpRowFileWithHeadersInputIterator(1, reader, iterator);

        rowFileWithHeadersInputIterator.next();
        verify(iterator).next();
    }

    interface Fixture {
        String expectedHeadersBlock = "Name,Age,City";
    }
}