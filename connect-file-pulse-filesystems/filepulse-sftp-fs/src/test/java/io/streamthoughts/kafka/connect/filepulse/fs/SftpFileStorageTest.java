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
package io.streamthoughts.kafka.connect.filepulse.fs;

import static java.time.Instant.ofEpochSecond;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.SftpATTRS;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.client.SftpClient;
import io.streamthoughts.kafka.connect.filepulse.fs.stream.ConnectionAwareInputStream;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SftpFileStorageTest {

    @Test
    @SneakyThrows
    void when_regular_file_exists_should_return_true() {
        SftpClient client = mock(SftpClient.class);

        SftpATTRS entryAttrs = mock(SftpATTRS.class);
        when(entryAttrs.isReg()).thenReturn(true);

        when(client.statFile(eq(Fixture.fUri.toString()))).thenReturn(entryAttrs);

        SftpFileStorage storage = new SftpFileStorage(client);
        assertThat(storage.exists(Fixture.fUri)).isTrue();
    }

    @Test
    @SneakyThrows
    void when_resources_is_not_a_regular_file_exists_should_return_false() {
        SftpClient client = mock(SftpClient.class);

        SftpATTRS entryAttrs = mock(SftpATTRS.class);
        when(entryAttrs.isReg()).thenReturn(false);

        when(client.statFile(eq(Fixture.fUri.toString()))).thenReturn(entryAttrs);

        SftpFileStorage storage = new SftpFileStorage(client);
        assertThat(storage.exists(Fixture.fUri)).isFalse();
    }

    @Test
    @SneakyThrows
    void when_resource_does_not_exists_getObjectMetadata_should_throw_exception() {
        SftpClient client = mock(SftpClient.class);
        when(client.getObjectMetadata(eq(Fixture.notExistsUri))).thenReturn(Stream.empty());

        SftpFileStorage storage = new SftpFileStorage(client);
        assertThatThrownBy(() -> storage.getObjectMetadata(Fixture.notExistsUri)).isInstanceOf(ConnectFilePulseException.class);
        verify(client).getObjectMetadata(Fixture.notExistsUri);
        verify(client, never()).buildFileMetadata(any());
    }

    @Test
    @SneakyThrows
    void when_resource_exists_getObjectMetadata_should_return_correct_FileObjectMeta() {

        SftpClient client = mock(SftpClient.class);

        when(client.getObjectMetadata(eq(Fixture.fUri))).thenReturn(Stream.of(Fixture.expectedFileObjectMeta));

        SftpFileStorage storage = new SftpFileStorage(client);
        assertThat(storage.getObjectMetadata(Fixture.fUri)).isEqualTo(Fixture.expectedFileObjectMeta);
        verify(client).getObjectMetadata(eq(Fixture.fUri));
    }

    @Test
    @SneakyThrows
    void when_getInputStream_is_called_it_should_delegate_to_sftpClient_sftpFileInputStream() {
        SftpClient client = mock(SftpClient.class);
        ConnectionAwareInputStream sftpStream = mock(ConnectionAwareInputStream.class);
        when(client.sftpFileInputStream(Fixture.fUri)).thenReturn(sftpStream);

        SftpFileStorage storage = new SftpFileStorage(client);
        InputStream result = storage.getInputStream(Fixture.fUri);
        assertThat(result).isEqualTo(sftpStream);
        verify(client).sftpFileInputStream(eq(Fixture.fUri));
    }

    @Test
    void when_move_is_called_it_should_delegate_to_sftpClient_move() {
        // Given
        SftpClient client = mock(SftpClient.class);
        when(client.move(Fixture.fUri, Fixture.tUri)).thenReturn(true);
        SftpFileStorage storage = new SftpFileStorage(client);

        // When
        boolean result = storage.move(Fixture.fUri, Fixture.tUri);

        // Then
        assertThat(result).isTrue();
        verify(client).move(eq(Fixture.fUri), eq(Fixture.tUri));
    }

    @Test
    @SneakyThrows
    void when_delete_is_called_it_should_delegate_to_sftpClient_delete() {
        SftpClient client = mock(SftpClient.class);
        when(client.delete(Fixture.fUri)).thenReturn(true);

        SftpFileStorage storage = new SftpFileStorage(client);
        boolean result = storage.delete(Fixture.fUri);
        assertThat(result).isTrue();
        verify(client).delete(eq(Fixture.fUri));
    }

    interface Fixture {
        String fName = "f.txt";
        String path = "/path";
        String destination = "/destination";
        URI fUri = URI.create(String.format("%s/%s", path, fName));
        URI tUri = URI.create(String.format("%s/%s", destination, fName));
        URI notExistsUri = URI.create(String.format("%s/%s", path, "this_file_does_not_exists.txt"));

        int entryMTime = (int) Instant.now().minus(2, DAYS).getEpochSecond();
        long entrySize = 1024;

        GenericFileObjectMeta expectedFileObjectMeta =
                new GenericFileObjectMeta.Builder()
                        .withName(fName)
                        .withUri(fUri)
                        .withLastModified(ofEpochSecond(entryMTime))
                        .withContentLength(entrySize)
                        .build();
    }
}