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
package io.streamthoughts.kafka.connect.filepulse.fs.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class SftpConnectionTest {
    public static SftpFilesystemListingConfig mockSftpFilesystemListingConfig() {
        SftpFilesystemListingConfig config = mock(SftpFilesystemListingConfig.class);
        when(config.getSftpListingHost()).thenReturn("host");
        when(config.getSftpListingUser()).thenReturn("user");
        when(config.getSftpListingPort()).thenReturn(1234);
        when(config.getSftpConnectionTimeoutMs()).thenReturn(5000);
        when(config.getSftpListingPassword()).thenReturn("pass");
        when(config.getSftpListingStrictHostKeyCheck()).thenReturn("false");

        return config;
    }

    @Test
    @SneakyThrows
    public void when_config_provided_initSession_should_initialized_session_successfully() {
        JSch jSch = mock(JSch.class);
        Session session = mock(Session.class);
        when(jSch.getSession(anyString(), anyString(), anyInt())).thenReturn(session);
        SftpFilesystemListingConfig config = mockSftpFilesystemListingConfig();

        SftpConnection sftpConnection = mock(SftpConnection.class);
        doReturn(jSch).when(sftpConnection).getJSch();
        doReturn(config).when(sftpConnection).getConfig();
        when(sftpConnection.initSession()).thenCallRealMethod();

        sftpConnection.initSession();

        verify(jSch).getSession(eq("user"), eq("host"), eq(1234));
        verify(session).setPassword(eq("pass"));
        verify(session).setConfig(eq("StrictHostKeyChecking"), eq("false"));
        verify(session).connect(eq(5000));
    }

    @Test
    @SneakyThrows
    public void when_getSession_throws_JschException_initSession_should_throw_ConnectFilePulseException() {
        JSch jSch = mock(JSch.class);
        when(jSch.getSession(anyString(), anyString(), anyInt())).thenThrow(JSchException.class);
        SftpFilesystemListingConfig config = mockSftpFilesystemListingConfig();

        SftpConnection sftpConnection = mock(SftpConnection.class);
        doReturn(jSch).when(sftpConnection).getJSch();
        doReturn(config).when(sftpConnection).getConfig();
        when(sftpConnection.initSession()).thenCallRealMethod();

        assertThatThrownBy(sftpConnection::initSession)
                .isInstanceOf(ConnectFilePulseException.class)
                .hasCauseInstanceOf(JSchException.class);
    }

    @Test
    @SneakyThrows
    void when_openChannel_throws_JschException_initChannel_should_throw_ConnectFilePulseException() {
        SftpFilesystemListingConfig config = mockSftpFilesystemListingConfig();
        Session session = mock(Session.class);
        SftpConnection sftpConnection = mock(SftpConnection.class);

        when(session.openChannel(eq(SftpClient.CHANNEL_TYPE))).thenThrow(JSchException.class);

        doReturn(session).when(sftpConnection).getSession();
        doReturn(config).when(sftpConnection).getConfig();
        when(sftpConnection.initChannel()).thenCallRealMethod();

        assertThatThrownBy(sftpConnection::initChannel)
                .isInstanceOf(ConnectFilePulseException.class)
                .hasCauseInstanceOf(JSchException.class);
    }
}
