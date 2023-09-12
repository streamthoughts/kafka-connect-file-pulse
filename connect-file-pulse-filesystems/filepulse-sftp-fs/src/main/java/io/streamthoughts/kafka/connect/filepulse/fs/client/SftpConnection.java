/*
 * Copyright 2019-2023 StreamThoughts.
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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpConnection implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SftpConnection.class);
    private static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    public static final String CHANNEL_TYPE = "sftp";

    private final SftpFilesystemListingConfig config;
    private final JSch jsch;
    private final Session session;
    private final ChannelSftp channel;

    public SftpConnection(SftpFilesystemListingConfig config) {
        this.config = config;
        this.jsch = initJsch();
        this.session = initSession();
        this.channel = initChannel();
    }

    JSch initJsch() {
        return new JSch();
    }

    Session initSession() {
        try {
            SftpFilesystemListingConfig conf = getConfig();
            Session jschSession = getJSch().getSession(conf.getSftpListingUser(), conf.getSftpListingHost(),
                    conf.getSftpListingPort());
            jschSession.setPassword(conf.getSftpListingPassword());
            jschSession.setConfig(STRICT_HOST_KEY_CHECKING, conf.getSftpListingStrictHostKeyCheck());
            jschSession.connect(conf.getSftpConnectionTimeoutMs());
            return jschSession;
        } catch (JSchException e) {
            throw new ConnectFilePulseException(buildConnectErrorMsg(), e);
        }
    }

    ChannelSftp initChannel() {
        try {
            SftpFilesystemListingConfig conf = getConfig();
            ChannelSftp channel = (ChannelSftp) getSession().openChannel(CHANNEL_TYPE);
            channel.connect(conf.getSftpConnectionTimeoutMs());
            return channel;
        } catch (JSchException e) {
            throw new ConnectFilePulseException(buildConnectErrorMsg(), e);
        }
    }

    public ChannelSftp getChannel() {
        return channel;
    }

    public JSch getJSch() {
        return jsch;
    }

    public Session getSession() {
        return session;
    }

    public SftpFilesystemListingConfig getConfig() {
        return config;
    }

    private String buildConnectErrorMsg() {
        SftpFilesystemListingConfig conf = getConfig();
        return String.format("Cannot connect as user %s to %s:%d",
                conf.getSftpListingUser(),
                conf.getSftpListingHost(),
                conf.getSftpListingPort());
    }

    @Override
    public void close() {
        Optional.ofNullable(session).filter(Session::isConnected).ifPresent(Session::disconnect);

        log.debug("Connection to {}:{} successfully closed.",
                config.getSftpListingHost(), config.getSftpListingPort());
    }
}
