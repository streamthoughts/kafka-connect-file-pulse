/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.client.SmbClient;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.io.InputStream;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Storage} for SMB/CIFS file systems.
 */
public class SmbFileStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(SmbFileStorage.class);

    private final SmbClient smbClient;

    public SmbFileStorage(SmbFileSystemListingConfig config) {
        this.smbClient = new SmbClient(config);
    }

    SmbFileStorage(SmbClient smbClient) {
        this.smbClient = smbClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta getObjectMetadata(URI uri) {
        LOG.debug("Getting object metadata for '{}'", uri);
        try {
            return smbClient.getObjectMetadata(uri);
        } catch (Exception e) {
            throw new ConnectFilePulseException(String.format("Cannot stat file with uri: %s", uri), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(URI uri) {
        LOG.debug("Checking if '{}' exists", uri);
        try {
            return smbClient.exists(uri);
        } catch (Exception e) {
            throw new ConnectFilePulseException(
                    String.format("Failed to check if SMB file exists: %s", uri), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(URI uri) {
        LOG.info("Deleting '{}'", uri);
        try {
            return smbClient.delete(uri);
        } catch (Exception e) {
            LOG.error("Failed to delete SMB file: {}", uri, e);
            throw new ConnectFilePulseException("Failed to delete SMB file: " + uri, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean move(URI source, URI dest) {
        LOG.info("Moving '{}' to '{}'", source, dest);
        try {
            return smbClient.move(source, dest);
        } catch (Exception e) {
            LOG.error("Failed to move SMB file from {} to {}", source, dest, e);
            throw new ConnectFilePulseException(
                    String.format("Failed to move SMB file from %s to %s", source, dest), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream(URI uri) {
        LOG.debug("Getting input stream for '{}'", uri);
        try {
            return smbClient.getInputStream(uri);
        } catch (Exception e) {
            LOG.error("Failed to get input stream for SMB file: {}", uri, e);
            throw new ConnectFilePulseException("Failed to get input stream for SMB file: " + uri, e);
        }
    }
}
