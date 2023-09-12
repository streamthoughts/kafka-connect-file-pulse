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

import static com.jcraft.jsch.ChannelSftp.SSH_FX_BAD_MESSAGE;
import static com.jcraft.jsch.ChannelSftp.SSH_FX_CONNECTION_LOST;
import static com.jcraft.jsch.ChannelSftp.SSH_FX_NO_CONNECTION;
import static org.apache.commons.io.FilenameUtils.getName;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.stream.ConnectionAwareInputStream;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpClient {

    private static final Logger log = LoggerFactory.getLogger(SftpClient.class);
    static final String IS_REGULAR_FILE = "is.regular.file";
    public static final String CHANNEL_TYPE = "sftp";
    static final List<Integer> TRANSIENT_SFTP_ERROR_CODES = List.of(
            SSH_FX_NO_CONNECTION, SSH_FX_CONNECTION_LOST, SSH_FX_BAD_MESSAGE);

    private final SftpFilesystemListingConfig config;

    /**
     * Initializes the underlying SFTP channel.
     *
     * @param config the connector configuration object.
     */
    public SftpClient(SftpFilesystemListingConfig config) {
        this.config = config;
    }

    public Stream<FileObjectMeta> listFiles(String path) {
        log.info("Listing files in path '{}'", path);
        return listAll(path)
                .filter(this::isRegularFile)
                .map(this::buildFileMetadata)
                .peek(meta -> log.debug("Found object '{}'", meta));
    }

    public Stream<LsEntry> listAll(String path) {
        return retryAction(() -> doInCloseableConnection(c -> listAllCore(path, c)));
    }

    @SuppressWarnings("unchecked")
    Stream<LsEntry> listAllCore(String path, SftpConnection connection) {
        try {
            ChannelSftp channel = connection.getChannel();
            return channel.ls(path).stream();
        } catch (SftpException e) {
            throw new ConnectFilePulseException("Cannot list files", e);
        }
    }

    public FileObjectMeta buildFileMetadata(LsEntry entry) {
        return buildFileMetadata(entry.getFilename(), entry.getAttrs());
    }

    FileObjectMeta buildFileMetadata(String filename, SftpATTRS attrs) {
        return new GenericFileObjectMeta.Builder()
                .withName(filename)
                .withUri(buildUri(filename))
                .withLastModified(Instant.ofEpochSecond(attrs.getMTime()))
                .withContentLength(attrs.getSize())
                .withUserDefinedMetadata(Map.of(IS_REGULAR_FILE, attrs.isReg()))
                .build();
    }

    private boolean isRegularFile(LsEntry e) {
        return e.getAttrs().isReg();
    }

    public SftpATTRS statFile(String absolutePath) {
        return retryAction(() -> doInCloseableConnection(c -> statFileCore(absolutePath, c)));
    }

    SftpATTRS statFileCore(String absolutePath, SftpConnection connection) {
        log.info("Getting attributes for file '{}'", absolutePath);
        try {
            ChannelSftp channel = connection.getChannel();
            return channel.stat(absolutePath);
        } catch (SftpException e) {
            throw new ConnectFilePulseException("Cannot stat file: " + absolutePath, e);
        }
    }

    public Stream<FileObjectMeta> getObjectMetadata(URI uri) {
        String absolutePath = uri.toString();
        final String filename = getName(absolutePath);
        return Stream.of((SftpATTRS) retryAction(() -> doInCloseableConnection(c -> statFileCore(absolutePath, c))))
                .map(attrs -> buildFileMetadata(filename, attrs));
    }

    /**
     * Builds a file URI as the absolute path from the sftp root.
     *
     * @param name the filename.
     * @return the absolute path of the file
     */
    public URI buildUri(String name) {
        return URI.create(String.format("%s/%s",
                config.getSftpListingDirectoryPath(),
                name));
    }

    /**
     * Instantiates an input stream on the remote file on the sftp.
     *
     * @param uri the URI representing a file on the SFTP
     * @return the inputstream.
     */
    public ConnectionAwareInputStream sftpFileInputStream(URI uri) {
        log.info("Getting InputStream for '{}'", uri);
        String absolutePath = uri.toString();

        return retryAction(() ->
                doInConnection(connection -> {
                    try {
                        ChannelSftp channel = connection.getChannel();
                        InputStream inputStream = channel.get(absolutePath);
                        return new ConnectionAwareInputStream(connection, absolutePath, inputStream);
                    } catch (SftpException e) {
                        log.error("Cannot open sftp InputStream for " + absolutePath, e);
                        throw new ConnectFilePulseException(e);
                    }
                }));
    }

    /**
     * Deletes the file from this storage.
     *
     * @param uri   the file {@link URI}.
     * @return      {@code true} if the file has been removed successfully, otherwise {@code false}.
     */
    public boolean delete(URI uri) {
        log.info("Deleting file on '{}'", uri);
        String absolutePath = uri.toString();

        return retryAction(() ->
                doInCloseableConnection(connection -> {
                    try {
                        ChannelSftp channel = connection.getChannel();
                        channel.rm(absolutePath);
                        return true;
                    } catch (SftpException e) {
                        log.error("Failed to remove file from " + uri, e);
                        if (isRetryableException(e)) {
                            throw new ConnectFilePulseException(e);
                        }
                    }
                    return false;
                })
        );
    }

    public boolean move(URI source, URI destination) {
        log.info("Moving file from '{}' to {} ", source, destination);
        return retryAction(() ->
                doInCloseableConnection(connection -> {
                    try {
                        ChannelSftp channel = connection.getChannel();
                        channel.rename(source.toString(), destination.toString());
                        return true;
                    } catch (SftpException e) {
                        log.error("Failed to move file from {} to {} ", source, destination, e);
                        if (isRetryableException(e)) {
                            throw new ConnectFilePulseException(e);
                        }
                    }
                    return false;
                })
        );
    }

    <R> R doInConnection(Function<SftpConnection, R> action) {
        SftpConnection sftpConnection = createSftpConnection();
        return action.apply(sftpConnection);
    }

    <R> R doInCloseableConnection(Function<SftpConnection, R> action) {
        try (SftpConnection sftpConnection = createSftpConnection()) {
            return action.apply(sftpConnection);
        }
    }

    SftpConnection createSftpConnection() {
        return new SftpConnection(config);
    }

    <T> T retryAction(Supplier<T> action) {
        try {
            return action.get();
        } catch (ConnectFilePulseException e) {
            return retryActionCore(config.getSftpRetries(), action, e);
        }
    }

    <T> T retryActionCore(Integer remainingRetries, Supplier<T> action, ConnectFilePulseException prevException) {
        if (remainingRetries == 0) {
            throw prevException;
        }

        log.debug("Retrying action, attempt {}", config.getSftpRetries() - remainingRetries + 1);

        try {
            return action.get();
        } catch (ConnectFilePulseException e) {
            wait(config);
            return retryActionCore(remainingRetries - 1, action, e);
        }
    }

    private void wait(SftpFilesystemListingConfig config) {
        try {
            Thread.sleep(config.getSftpDelayBetweenRetriesMs());
        } catch (InterruptedException e) {
            throw new ConnectFilePulseException(buildConnectErrorMsg(), e);
        }
    }

    private String buildConnectErrorMsg() {
        return String.format("Cannot connect as user %s to %s:%d",
                config.getSftpListingUser(),
                config.getSftpListingHost(),
                config.getSftpListingPort());
    }

    private static boolean isRetryableException(SftpException e) {
        return TRANSIENT_SFTP_ERROR_CODES.stream().anyMatch(i -> e.id == i);
    }
}
