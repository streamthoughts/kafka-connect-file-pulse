/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.client.SmbClient;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SmbFileStorageTest {

    @Test
    @SneakyThrows
    void when_exists_delegates_to_client_and_returns_result() {
        SmbClient client = mock(SmbClient.class);
        when(client.exists(Fixture.FILE_URI)).thenReturn(true);

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThat(storage.exists(Fixture.FILE_URI)).isTrue();
        verify(client).exists(eq(Fixture.FILE_URI));
    }

    @Test
    void when_exists_throws_exception_it_should_be_wrapped_in_ConnectFilePulseException() {
        SmbClient client = mock(SmbClient.class);
        when(client.exists(Fixture.FILE_URI)).thenThrow(new RuntimeException("boom"));

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThatThrownBy(() -> storage.exists(Fixture.FILE_URI))
                .isInstanceOf(ConnectFilePulseException.class);
    }

    @Test
    @SneakyThrows
    void when_getObjectMetadata_delegates_to_client_and_returns_metadata() {
        SmbClient client = mock(SmbClient.class);
        FileObjectMeta meta = mock(FileObjectMeta.class);
        when(client.getObjectMetadata(Fixture.FILE_URI)).thenReturn(meta);

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThat(storage.getObjectMetadata(Fixture.FILE_URI)).isEqualTo(meta);
        verify(client).getObjectMetadata(eq(Fixture.FILE_URI));
    }

    @Test
    void when_getObjectMetadata_throws_exception_it_should_be_wrapped_in_ConnectFilePulseException() {
        SmbClient client = mock(SmbClient.class);
        when(client.getObjectMetadata(Fixture.FILE_URI)).thenThrow(new RuntimeException("boom"));

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThatThrownBy(() -> storage.getObjectMetadata(Fixture.FILE_URI))
                .isInstanceOf(ConnectFilePulseException.class);
    }

    @Test
    @SneakyThrows
    void when_getInputStream_delegates_to_client_and_returns_stream() {
        SmbClient client = mock(SmbClient.class);
        InputStream is = mock(InputStream.class);
        when(client.getInputStream(Fixture.FILE_URI)).thenReturn(is);

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThat(storage.getInputStream(Fixture.FILE_URI)).isEqualTo(is);
        verify(client).getInputStream(eq(Fixture.FILE_URI));
    }

    @Test
    void when_getInputStream_throws_exception_it_should_be_wrapped_in_ConnectFilePulseException() {
        SmbClient client = mock(SmbClient.class);
        when(client.getInputStream(Fixture.FILE_URI)).thenThrow(new RuntimeException("boom"));

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThatThrownBy(() -> storage.getInputStream(Fixture.FILE_URI))
                .isInstanceOf(ConnectFilePulseException.class);
    }

    @Test
    void when_delete_is_called_it_should_delegate_to_client_delete() {
        SmbClient client = mock(SmbClient.class);
        when(client.delete(Fixture.FILE_URI)).thenReturn(true);

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThat(storage.delete(Fixture.FILE_URI)).isTrue();
        verify(client).delete(eq(Fixture.FILE_URI));
    }

    @Test
    void when_move_is_called_it_should_delegate_to_client_move() {
        SmbClient client = mock(SmbClient.class);
        when(client.move(Fixture.FILE_URI, Fixture.DEST_URI)).thenReturn(true);

        SmbFileStorage storage = new SmbFileStorage(client);

        assertThat(storage.move(Fixture.FILE_URI, Fixture.DEST_URI)).isTrue();
        verify(client).move(eq(Fixture.FILE_URI), eq(Fixture.DEST_URI));
    }

    interface Fixture {
        URI FILE_URI = URI.create("smb://server/share/" + UUID.randomUUID());
        URI DEST_URI = URI.create("smb://server/share/dest/" + UUID.randomUUID());
    }
}
