/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import com.jcraft.jsch.SftpATTRS;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.client.SftpClient;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpFileStorage implements Storage {
    private static final Logger log = LoggerFactory.getLogger(SftpFileStorage.class);
    private static final String CANNOT_STAT_FILE_ERROR_MSG_TEMPLATE = "Cannot stat file with uri: %s";

    private final SftpClient sftpClient;

    public SftpFileStorage(SftpFilesystemListingConfig config) {
        this.sftpClient = new SftpClient(config);
    }

    SftpFileStorage(SftpClient sftpClient) {
        this.sftpClient = sftpClient;
    }

    @Override
    public FileObjectMeta getObjectMetadata(URI uri) {
        log.debug("Getting object metadata for '{}'", uri);
        return sftpClient.getObjectMetadata(uri)
                .findFirst().orElseThrow(() -> new ConnectFilePulseException(buildCannotStatFileErrorMsg(uri)));
    }

    @Override
    public boolean exists(URI uri) {
        log.info("Checking if '{}' exists", uri);
        SftpATTRS attrs = sftpClient.statFile(uri.toString());

        return attrs.isReg();
    }

    private String buildCannotStatFileErrorMsg(URI uri) {
        return String.format(CANNOT_STAT_FILE_ERROR_MSG_TEMPLATE, uri);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(URI uri) {
        return sftpClient.delete(uri);
    }

    @Override
    public boolean move(URI source, URI dest) {
        return sftpClient.move(source, dest);
    }

    @Override
    public InputStream getInputStream(URI uri) {
        return sftpClient.sftpFileInputStream(uri);
    }
}
