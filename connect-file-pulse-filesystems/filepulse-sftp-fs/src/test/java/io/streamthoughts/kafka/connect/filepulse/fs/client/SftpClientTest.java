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
package io.streamthoughts.kafka.connect.filepulse.fs.client;

import static com.jcraft.jsch.ChannelSftp.SSH_FX_NO_SUCH_FILE;
import static com.jcraft.jsch.ChannelSftp.SSH_FX_OP_UNSUPPORTED;
import static com.jcraft.jsch.ChannelSftp.SSH_FX_PERMISSION_DENIED;
import static io.streamthoughts.kafka.connect.filepulse.fs.client.SftpClient.IS_REGULAR_FILE;
import static io.streamthoughts.kafka.connect.filepulse.fs.client.SftpClient.TRANSIENT_SFTP_ERROR_CODES;
import static java.time.Instant.ofEpochSecond;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.stream.ConnectionAwareInputStream;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SftpClientTest {

    @SneakyThrows
    private ChannelSftp mockChannel(SftpClient mockClient) {
        SftpConnection sftpConnection = mock(SftpConnection.class);
        ChannelSftp channel = mock(ChannelSftp.class);
        when(sftpConnection.getChannel()).thenReturn(channel);
        doReturn(sftpConnection).when(mockClient).createSftpConnection();

        return channel;
    }

    @SneakyThrows
    public static SftpClient mockSftpClient(SftpFilesystemListingConfig config) {
        return spy(new SftpClient(config));
    }

    @Test
    @SneakyThrows
    void when_remote_entries_listFiles_should_return_their_metadata() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = spy(new SftpClient(config));

        ChannelSftp channelSftp = mockChannel(client);

        ChannelSftp.LsEntry entry0 = mockEntry(Fixture.f0Name, Fixture.entryMTime, Fixture.entrySize);
        ChannelSftp.LsEntry entry1 = mockEntry(Fixture.f1Name, Fixture.entryMTime + 1000, Fixture.entrySize * 2);
        ChannelSftp.LsEntry entry2 = mockEntry(Fixture.f2Name, Fixture.entryMTime + 2000, Fixture.entrySize * 4);

        Vector<ChannelSftp.LsEntry> entries = new Vector<>();
        entries.add(entry0);
        entries.add(entry1);
        entries.add(entry2);

        when(channelSftp.ls(anyString())).thenReturn(entries);

        List<FileObjectMeta> result = client.listFiles(Fixture.path).collect(Collectors.toList());

        assertEquals(3, result.size());
    }

    @Test
    @SneakyThrows
    void when_sftp_client_initialized_buildUri_should_create_uris_based_the_root_folder() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        when(config.getSftpListingDirectoryPath()).thenReturn("/test/path");

        SftpClient client = new SftpClient(config);
        URI expectedUri = URI.create("/test/path/filename.txt");
        assertEquals(expectedUri, client.buildUri("filename.txt"));
    }

    @ParameterizedTest
    @MethodSource
    @SneakyThrows
    void when_content_provided_sftpFileInputStream_should_initialize_stream_successfully(String filePath, Boolean isZipped) {
        URI testData = new URI(filePath);
        InputStream raw = getClass().getClassLoader().getResourceAsStream(testData.toString());

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);

        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);
        when(channelSftp.get(anyString())).thenReturn(raw);

        ConnectionAwareInputStream sftpStream = client.sftpFileInputStream(testData);

        assertNotNull(sftpStream);
        assertEquals(isZipped, sftpStream.isContentZipped());
    }

    @Test
    @SneakyThrows
    void when_filename_ends_with_zip_but_content_is_not_zipped_sftpFileInputStream_should_throw_exception() {
        URI testData = new URI("data/fake_zip.csv.zip");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(testData.toString());

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);

        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);
        when(channelSftp.get(anyString())).thenReturn(raw);

        Assertions.assertThatThrownBy(() -> client.sftpFileInputStream(testData))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasNoCause();
    }

    @Test
    @SneakyThrows
    void when_underlying_inputstream_is_closed_then_sftpFileInputStream_should_throw_exception() {
        URI testData = new URI("data/test_data.csv.zip");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(testData.toString());
        raw.close();

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);

        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);
        when(channelSftp.get(anyString())).thenReturn(raw);

        Assertions.assertThatThrownBy(() -> client.sftpFileInputStream(testData))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    @SneakyThrows
    void when_resource_does_not_exists_sftpFileInputStream_should_throw_exception() {
        URI notExistingUri = new URI("data/this_file_does_not_exists.csv");
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);
        when(channelSftp.get(anyString())).thenThrow(new SftpException(0, "test exception"));

        Assertions.assertThatThrownBy(() -> client.sftpFileInputStream(notExistingUri))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasRootCauseInstanceOf(SftpException.class);
    }

    @Test
    @SneakyThrows
    void when_sftp_client_initialized_buildFileMetadata_should_return_file_metadata() {
        ChannelSftp.LsEntry entry = mockEntry(Fixture.f0Name, Fixture.entryMTime, Fixture.entrySize);

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        when(config.getSftpListingDirectoryPath()).thenReturn(Fixture.path);

        SftpClient client = spy(new SftpClient(config));

        FileObjectMeta meta = client.buildFileMetadata(entry);

        assertEquals(Fixture.expectedMetadata, meta);
    }

    @Test
    @SneakyThrows
    void when_action_returns_a_value_doInConnection_should_return_that_value() {
        Function<SftpConnection, String> action = mock(Function.class);
        when(action.apply(any())).thenReturn(Fixture.expectedActionResult);

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpConnection sftpConnection = mock(SftpConnection.class);
        SftpClient client = mockSftpClient(config);
        doReturn(sftpConnection).when(client).createSftpConnection();

        assertThat(client.doInConnection(action)).isEqualTo(Fixture.expectedActionResult);
    }

    @Test
    @SneakyThrows
    void when_action_throws_unexpected_exception_doInConnection_should_rethrow_exception() {
        Function<SftpConnection, Object> action = mock(Function.class);
        when(action.apply(any())).thenThrow(RuntimeException.class);

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        SftpConnection sftpConnection = mock(SftpConnection.class);
        doReturn(sftpConnection).when(client).createSftpConnection();

        assertThatThrownBy(() -> client.doInConnection(action))
                .isInstanceOf(RuntimeException.class)
                .hasNoCause();
    }

    @Test
    void when_called_doInNewConnection_should_force_closing_connection() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        SftpConnection sftpConnection = mock(SftpConnection.class);
        Function<SftpConnection, String> action = mock(Function.class);

        doReturn(sftpConnection).when(client).createSftpConnection();
        doReturn(Fixture.expectedActionResult).when(action).apply(any());

        String res = client.doInCloseableConnection(action);

        verify(sftpConnection).close();
        assertThat(res).isEqualTo(Fixture.expectedActionResult);
    }

    @SneakyThrows
    @Test
    void when_channel_stat_throws_SftpException_statFileCore_should_throw_ConnectFilepulseException() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        ChannelSftp channel = mock(ChannelSftp.class);
        SftpConnection connection = mock(SftpConnection.class);
        when(connection.getChannel()).thenReturn(channel);
        when(channel.stat(anyString())).thenThrow(SftpException.class);

        SftpClient client = mockSftpClient(config);

        assertThrows(ConnectFilePulseException.class, () -> client.statFileCore(Fixture.f0Name, connection));
    }

    @SneakyThrows
    @Test
    void when_channel_stat_returns_data_statFileCore_should_throw_ConnectFilepulseException() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        ChannelSftp channel = mock(ChannelSftp.class);
        SftpATTRS entryAttrs = mock(SftpATTRS.class);
        when(channel.stat(anyString())).thenReturn(entryAttrs);
        SftpConnection connection = mock(SftpConnection.class);
        when(connection.getChannel()).thenReturn(channel);

        SftpClient client = mockSftpClient(config);

        assertThat(client.statFileCore(Fixture.f0Name, connection)).isEqualTo(entryAttrs);
    }

    @Test
    @SneakyThrows
    void when_statFile_called_doInNewConnection_should_delegate_to_statFileCore() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channel = mock(ChannelSftp.class);

        SftpConnection sftpConnection = mock(SftpConnection.class);
        when(sftpConnection.getChannel()).thenReturn(channel);
        doReturn(sftpConnection).when(client).createSftpConnection();

        client.statFile(Fixture.f1Uri.toString());

        verify(client).doInCloseableConnection(any());
        verify(client).statFileCore(eq(Fixture.f1Uri.toString()), eq(sftpConnection));
        verify(sftpConnection).close();
    }

    @Test
    @SneakyThrows
    void when_getObjectMeta_called_doInNewConnection_should_delegate_to_statFileCore() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channel = mock(ChannelSftp.class);

        SftpConnection sftpConnection = mock(SftpConnection.class);
        when(sftpConnection.getChannel()).thenReturn(channel);
        doReturn(sftpConnection).when(client).createSftpConnection();

        SftpATTRS entryAttrs = mock(SftpATTRS.class);
        when(entryAttrs.getMTime()).thenReturn(Fixture.entryMTime);
        when(entryAttrs.getSize()).thenReturn(Fixture.entrySize);
        when(entryAttrs.isReg()).thenReturn(true);
        when(channel.stat(anyString())).thenReturn(entryAttrs);

        client.getObjectMetadata(Fixture.f1Uri).collect(Collectors.toList());

        verify(client).doInCloseableConnection(any());
        verify(client).statFileCore(eq(Fixture.f1Uri.toString()), eq(sftpConnection));
        verify(client).buildFileMetadata(eq(Fixture.f1Name), eq(entryAttrs));
        verify(sftpConnection).close();
    }

    public static Stream<Arguments> when_content_provided_sftpFileInputStream_should_initialize_stream_successfully() {
        return Stream.of(
                arguments("data/test_data.csv", false),
                arguments("data/test_data.csv.zip", true)
        );
    }

    @Test
    @SneakyThrows
    void when_filename_ends_with_zip_but_content_is_not_zipped_and_retry_is_configured_sftpFileInputStream_should_retry_and_throw_exception() {
        URI testData = new URI("data/fake_zip.csv.zip");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(testData.toString());

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        ChannelSftp channelSftp = mock(ChannelSftp.class);
        when(config.getSftpRetries()).thenReturn(1);

        when(channelSftp.get(anyString())).thenReturn(raw);

        SftpConnection sftpConnection = mock(SftpConnection.class);
        doReturn(channelSftp).when(sftpConnection).getChannel();

        SftpClient client = spy(new SftpClient(config));
        doReturn(sftpConnection).when(client).createSftpConnection();

        Assertions.assertThatThrownBy(() -> client.sftpFileInputStream(testData))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasNoCause();

        verify(client, times(2)).retryActionCore(any(), any(), any());
    }

    @Test
    @SneakyThrows
    void when_underlying_inputstream_is_closed_and_retry_is_configured_then_sftpFileInputStream_should_retry_and_throw_exception() {
        URI testData = new URI("data/test_data.csv.zip");
        InputStream raw = getClass().getClassLoader().getResourceAsStream(testData.toString());
        raw.close();

        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        ChannelSftp channelSftp = mock(ChannelSftp.class);
        when(config.getSftpRetries()).thenReturn(2);

        SftpConnection sftpConnection = mock(SftpConnection.class);
        doReturn(channelSftp).when(sftpConnection).getChannel();

        when(channelSftp.get(anyString())).thenReturn(raw);

        SftpClient client = spy(new SftpClient(config));
        doReturn(sftpConnection).when(client).createSftpConnection();

        Assertions.assertThatThrownBy(() -> client.sftpFileInputStream(testData))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasCauseInstanceOf(IOException.class);

        verify(client, times(3)).retryActionCore(any(), any(), any());
    }

    @Test
    @SneakyThrows
    void when_resource_does_not_exists_and_retry_is_configured_sftpFileInputStream_should_retry_and_throw_exception() {
        URI notExistingUri = new URI("data/this_file_does_not_exists.csv");

        ChannelSftp channelSftp = mock(ChannelSftp.class);
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        when(config.getSftpRetries()).thenReturn(3);

        SftpConnection sftpConnection = mock(SftpConnection.class);
        doReturn(channelSftp).when(sftpConnection).getChannel();
        when(channelSftp.get(anyString())).thenThrow(new SftpException(0, "test exception"));

        SftpClient client = spy(new SftpClient(config));
        doReturn(sftpConnection).when(client).createSftpConnection();

        Assertions.assertThatThrownBy(() -> client.sftpFileInputStream(notExistingUri))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasCauseInstanceOf(SftpException.class);

        verify(client, times(4)).retryActionCore(any(), any(), any());
    }

    @Test
    @SneakyThrows
    void when_underlying_channel_removes_file_then_client_should_return_true() {
        // Given
        URI testData = new URI("data/test_data.csv.zip");
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);

        doNothing().when(channelSftp).rm(anyString());

        // When
        boolean result = client.delete(testData);

        // Then
        assertThat(result).isTrue();
        verify(channelSftp).rm(eq(testData.toString()));
    }

    @ParameterizedTest
    @MethodSource("permanent_sftp_exceptions")
    @SneakyThrows
    void when_underlying_channel_throws_permanent_exception_while_removing_file_then_client_should_return_false(int errorCode) {
        // Given
        URI testData = new URI("data/test_data.csv.zip");
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);

        doThrow(new SftpException(errorCode, "SFTP Channel Exception")).when(channelSftp).rm(anyString());

        // When
        boolean result = client.delete(testData);

        // Then
        assertThat(result).isFalse();
        verify(channelSftp).rm(eq(testData.toString()));
    }

    public static Stream<Arguments> permanent_sftp_exceptions() {
        return Stream.of(
                arguments(SSH_FX_NO_SUCH_FILE),
                arguments(SSH_FX_OP_UNSUPPORTED),
                arguments(SSH_FX_PERMISSION_DENIED)
        );
    }

    @ParameterizedTest
    @MethodSource("transient_sftp_exceptions")
    @SneakyThrows
    void when_underlying_channel_throws_transient_exception_while_removing_file_then_client_should_retry_and_throw_exception(int errorCode) {
        // Given
        URI testData = new URI("data/test_data.csv.zip");
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        when(config.getSftpRetries()).thenReturn(1);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);

        doThrow(new SftpException(errorCode, "SFTP Channel Exception")).when(channelSftp).rm(anyString());

        // When
        Assertions.assertThatThrownBy(() -> client.delete(testData))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasCauseInstanceOf(SftpException.class);

        // Then
        verify(channelSftp, times(2)).rm(eq(testData.toString()));
    }

    public static Stream<Arguments> transient_sftp_exceptions() {
        return TRANSIENT_SFTP_ERROR_CODES.stream().map(Arguments::arguments);
    }

    @Test
    @SneakyThrows
    void when_underlying_channel_moves_file_then_client_should_return_true() {
        // Given
        URI source = new URI("source/test_data.csv");
        URI destination = new URI("destination/test_data.csv");
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);

        doNothing().when(channelSftp).rename(anyString(), anyString());

        // When
        boolean result = client.move(source, destination);

        // Then
        assertThat(result).isTrue();
        verify(channelSftp).rename(eq(source.toString()), eq(destination.toString()));
    }

    @ParameterizedTest
    @MethodSource("permanent_sftp_exceptions")
    @SneakyThrows
    void when_underlying_channel_throws_permanent_exception_while_moving_files_then_client_should_return_false(int errorCode) {
        // Given
        URI source = new URI("source/test_data.csv");
        URI destination = new URI("destination/test_data.csv");
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);

        doThrow(new SftpException(errorCode, "SFTP Channel Exception")).when(channelSftp).rename(anyString(), anyString());

        // When
        boolean result = client.move(source, destination);

        // Then
        assertThat(result).isFalse();
        verify(channelSftp).rename(eq(source.toString()), eq(destination.toString()));
    }

    @ParameterizedTest
    @MethodSource("transient_sftp_exceptions")
    @SneakyThrows
    void when_underlying_channel_throws_transient_exception_while_moving_file_then_client_should_retry_and_throw_exception(int errorCode) {
        // Given
        URI source = new URI("source/test_data.csv");
        URI destination = new URI("destination/test_data.csv");
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        when(config.getSftpRetries()).thenReturn(1);
        SftpClient client = mockSftpClient(config);
        ChannelSftp channelSftp = mockChannel(client);

        doThrow(new SftpException(errorCode, "SFTP Channel Exception")).when(channelSftp).rename(anyString(), anyString());

        // When
        Assertions.assertThatThrownBy(() -> client.move(source, destination))
                .isInstanceOf(ConnectFilePulseException.class)
                .hasCauseInstanceOf(SftpException.class);

        // Then
        verify(channelSftp, times(2)).rename(eq(source.toString()), eq(destination.toString()));
    }

    public static ChannelSftp.LsEntry mockEntry(String fileName, int mTime, long size) {
        ChannelSftp.LsEntry entry = mock(ChannelSftp.LsEntry.class);
        when(entry.getFilename()).thenReturn(fileName);

        SftpATTRS entryAttrs = mock(SftpATTRS.class);
        when(entryAttrs.getMTime()).thenReturn(mTime);
        when(entryAttrs.getSize()).thenReturn(size);
        when(entryAttrs.isReg()).thenReturn(true);
        when(entry.getAttrs()).thenReturn(entryAttrs);
        return entry;
    }

    interface Fixture {
        String f0Name = "f0.txt";
        String f1Name = "f0.txt";
        String f2Name = "f0.txt";
        String path = "/path";
        URI f1Uri = URI.create(String.format("%s/%s", path, f0Name));

        int entryMTime = (int) Instant.now().getEpochSecond();
        long entrySize = 1024;

        String expectedActionResult = "action_result";

        GenericFileObjectMeta expectedMetadata =
                new GenericFileObjectMeta.Builder()
                        .withName(f0Name)
                        .withUri(f1Uri)
                        .withLastModified(ofEpochSecond(entryMTime))
                        .withContentLength(entrySize)
                        .withUserDefinedMetadata(Map.of(IS_REGULAR_FILE, true))
                        .build();
    }
}