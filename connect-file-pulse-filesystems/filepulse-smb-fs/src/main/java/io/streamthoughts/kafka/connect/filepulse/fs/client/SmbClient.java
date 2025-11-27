/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.client;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.SmbFileSystemListingConfig;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import jcifs.CIFSContext;
import jcifs.CIFSException;
import jcifs.config.PropertyConfiguration;
import jcifs.context.BaseContext;
import jcifs.smb.NtlmPasswordAuthenticator;
import jcifs.smb.SmbFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SMB client wrapper for handling SMB/CIFS connections and operations.
 */
public class SmbClient {

    private static final Logger LOG = LoggerFactory.getLogger(SmbClient.class);

    private final SmbFileSystemListingConfig config;
    private final CIFSContext cifsContext;
    private final String baseUrl;

    public SmbClient(SmbFileSystemListingConfig config) {
        this.config = config;
        this.cifsContext = createCifsContext();
        this.baseUrl = buildBaseUrl();
    }

    /**
     * Create CIFS context with authentication and configuration.
     */
    private CIFSContext createCifsContext() {
        try {
            Properties props = new Properties();

            // Configure SMB protocol versions
            props.setProperty("jcifs.smb.client.maxVersion", config.getSmbMaxVersion());
            props.setProperty("jcifs.smb.client.minVersion", "SMB300");

            // Configure timeouts
            props.setProperty("jcifs.smb.client.responseTimeout", String.valueOf(config.getResponseTimeout()));
            props.setProperty("jcifs.smb.client.connTimeout", String.valueOf(config.getConnectionTimeout()));
            props.setProperty("jcifs.smb.client.soTimeout", String.valueOf(config.getSoTimeout()));

            PropertyConfiguration propConfig = new PropertyConfiguration(props);
            BaseContext baseContext = new BaseContext(propConfig);

            // Create authentication credentials
            NtlmPasswordAuthenticator auth = new NtlmPasswordAuthenticator(
                    config.getSmbDomain(),
                    config.getSmbUser(),
                    config.getSmbPassword()
            );

            return baseContext.withCredentials(auth);
        } catch (CIFSException e) {
            throw new ConnectFilePulseException("Failed to create CIFS context", e);
        }
    }

    /**
     * Build base SMB URL from configuration.
     */
    private String buildBaseUrl() {
        String host = config.getSmbHost();
        String share = config.getSmbShare();

        // Ensure share doesn't start with /
        if (share.startsWith("/")) {
            share = share.substring(1);
        }

        return String.format("smb://%s/%s/", host, share);
    }

    /**
     * List files in the specified directory.
     */
    public Stream<FileObjectMeta> listFiles(String directoryPath) {
        String smbUrl = baseUrl + (directoryPath.startsWith("/") ? directoryPath.substring(1) : directoryPath);

        return executeWithRetry(() -> {
            LOG.debug("Listing files in SMB directory: {}", smbUrl);

            try (SmbFile directory = new SmbFile(smbUrl, cifsContext)) {
                if (!directory.exists()) {
                    throw new ConnectFilePulseException("SMB directory does not exist: " + smbUrl);
                }

                if (!directory.isDirectory()) {
                    throw new ConnectFilePulseException("SMB path is not a directory: " + smbUrl);
                }

                SmbFile[] files = directory.listFiles();
                if (files == null) {
                    return Stream.empty();
                }

                return Stream.of(files)
                        .filter(this::isRegularFile)
                        .map(this::buildFileMetadata);
            }
        });
    }

    /**
     * Check if SMB file is a regular file (not directory).
     */
    private boolean isRegularFile(SmbFile file) {
        try {
            return file.isFile();
        } catch (Exception e) {
            LOG.warn("Failed to check if SMB file is regular: {}", file.getPath(), e);
            return false;
        }
    }

    /**
     * Build file metadata from SMB file.
     */
    private FileObjectMeta buildFileMetadata(SmbFile file) {
        try {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("smb.server", config.getSmbHost());
            metadata.put("smb.share", config.getSmbShare());

            return new GenericFileObjectMeta.Builder()
                    .withUri(file.getURL().toURI())
                    .withName(file.getName())
                    .withContentLength(file.length())
                    .withLastModified(file.getLastModified())
                    .withUserDefinedMetadata(metadata)
                    .build();
        } catch (Exception e) {
            throw new ConnectFilePulseException("Failed to build file metadata for: " + file.getPath(), e);
        }
    }

    /**
     * Get metadata for a specific file.
     */
    public FileObjectMeta getObjectMetadata(URI uri) {
        return executeWithRetry(() -> {
            String smbUrl = uri.toString();
            LOG.debug("Getting metadata for SMB file: {}", smbUrl);

            try (SmbFile file = new SmbFile(smbUrl, cifsContext)) {
                if (!file.exists()) {
                    throw new ConnectFilePulseException("SMB file does not exist: " + smbUrl);
                }

                return buildFileMetadata(file);
            }
        });
    }

    /**
     * Check if file exists.
     */
    public boolean exists(URI uri) {
        return executeWithRetry(() -> {
            String smbUrl = uri.toString();
            LOG.debug("Checking if SMB file exists: {}", smbUrl);

            try (SmbFile file = new SmbFile(smbUrl, cifsContext)) {
                return file.exists() && file.isFile();
            }
        });
    }

    /**
     * Delete file.
     */
    public boolean delete(URI uri) {
        return executeWithRetry(() -> {
            String smbUrl = uri.toString();
            LOG.info("Deleting SMB file: {}", smbUrl);

            try (SmbFile file = new SmbFile(smbUrl, cifsContext)) {
                if (!file.exists()) {
                    LOG.warn("Cannot delete SMB file - does not exist: {}", smbUrl);
                    return false;
                }

                file.delete();
                return true;
            }
        });
    }

    /**
     * Move/rename file.
     */
    public boolean move(URI source, URI dest) {
        return executeWithRetry(() -> {
            String sourceSmbUrl = source.toString();
            String destSmbUrl = dest.toString();
            LOG.info("Moving SMB file from {} to {}", sourceSmbUrl, destSmbUrl);

            try (SmbFile sourceFile = new SmbFile(sourceSmbUrl, cifsContext);
                 SmbFile destFile = new SmbFile(destSmbUrl, cifsContext)) {

                if (!sourceFile.exists()) {
                    throw new ConnectFilePulseException("Source SMB file does not exist: " + sourceSmbUrl);
                }

                sourceFile.renameTo(destFile);
                return true;
            }
        });
    }

    /**
     * Get input stream for file.
     */
    public InputStream getInputStream(URI uri) {
        return executeWithRetry(() -> {
            String smbUrl = uri.toString();
            LOG.debug("Opening input stream for SMB file: {}", smbUrl);

            SmbFile file = new SmbFile(smbUrl, cifsContext);

            if (!file.exists()) {
                throw new ConnectFilePulseException("SMB file does not exist: " + smbUrl);
            }

            return file.getInputStream();
        });
    }

    /**
     * Execute operation with retry logic.
     */
    private <T> T executeWithRetry(SmbOperation<T> operation) {
        int retries = config.getConnectionRetries();
        int delay = config.getConnectionRetriesDelay();

        Exception lastException = null;

        for (int attempt = 0; attempt <= retries; attempt++) {
            try {
                return operation.execute();
            } catch (Exception e) {
                lastException = e;

                if (!isRetryable(e) || attempt == retries) {
                    LOG.error("SMB operation failed after {} attempts", attempt + 1, e);
                    throw new ConnectFilePulseException(
                            "SMB operation failed after " + (attempt + 1) + " attempts", e);
                }

                LOG.warn("SMB operation failed (attempt {}/{}), retrying in {}ms: {}",
                        attempt + 1, retries + 1, delay, e.getMessage());

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ConnectFilePulseException("SMB operation interrupted", ie);
                }
            }
        }

        throw new ConnectFilePulseException("SMB operation failed", lastException);
    }

    private boolean isRetryable(Throwable t) {
        if (t instanceof ConnectFilePulseException && t.getCause() != null) {
            return isRetryable(t.getCause());
        }
        if (t instanceof IOException && !(t instanceof CIFSException)) {
            return true;
        }
        if (t instanceof CIFSException) {
            // For now, be conservative: consider network-like CIFS issues retryable,
            // but leave room to refine based on status codes if needed.
            return true;
        }
        return false;
    }

    /**
     * Functional interface for SMB operations.
     */
    @FunctionalInterface
    private interface SmbOperation<T> {
        T execute() throws IOException;
    }
}
