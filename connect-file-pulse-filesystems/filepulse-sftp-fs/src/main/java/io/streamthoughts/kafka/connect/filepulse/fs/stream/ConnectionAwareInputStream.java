/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.stream;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.client.SftpConnection;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionAwareInputStream extends InputStream {

    private static final Logger log = LoggerFactory.getLogger(ConnectionAwareInputStream.class);
    private static final String ZIP_EXTENSION = ".zip";
    final SftpConnection connection;
    final String absolutePath;
    final InputStream delegate;

    public ConnectionAwareInputStream(SftpConnection connection, String absolutePath, InputStream delegate) {
        this.connection = connection;
        this.absolutePath = absolutePath;
        this.delegate = isContentZipped() ? wrapAsCompressedStream(delegate) : delegate;
    }

    private InputStream wrapAsCompressedStream(InputStream raw) {
        try {
            return wrapAsCompressedStreamCore(raw);
        } catch (IOException ioe) {
            log.error("Cannot wrap InputStream into a compressed stream for " + absolutePath, ioe);
            throw new ConnectFilePulseException(ioe);
        }
    }

    /**
     * Wraps the raw stream into the appropriate specific compressed stream.
     *
     * @param raw          the underlying input stream.
     * @return the wrapped input stream if appropriate, the raw stream otherwise.
     * @throws IOException if the file is compressed but for some reason the compressed stream cannot be created.
     */
    private InputStream wrapAsCompressedStreamCore(InputStream raw) throws IOException {
        log.debug("Input file is a ZIP, embedding InputStream into a ZipInputStream");
        ZipInputStream zipInputStream = new ZipInputStream(raw, StandardCharsets.UTF_8);
        ZipEntry zipEntry = zipInputStream.getNextEntry();

        return Optional.ofNullable(zipEntry)
                .map(__ -> zipInputStream)
                .orElseThrow(() -> new ConnectFilePulseException(
                        String.format("Zip file '%s' has no content", absolutePath)
                ));
    }

    public boolean isContentZipped() {
        return absolutePath.toLowerCase().endsWith(ZIP_EXTENSION);
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public int read() throws IOException {
        return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return delegate.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

}
