/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.source;

import com.jsoniter.annotation.JsonIgnore;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.internal.IOUtils;
import io.streamthoughts.kafka.connect.filepulse.internal.Network;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

/**
 * Immutable class used to wrap all input source file metadata.
 */
public class LocalFileObjectMeta extends GenericFileObjectMeta {

    public static final String SYSTEM_FILE_INODE_META_KEY = "system.inode";
    public static final String SYSTEM_FILE_HOSTNAME_META_KEY = "system.hostname";

    @JsonIgnore
    private final File file;

    @JsonIgnore
    private final Long inode;

    /**
     * Creates a new {@link LocalFileObjectMeta} instance.
     *
     * @param file       the {@link File} object?.
     */
    public LocalFileObjectMeta(final File file) {
        super(
            file.toURI(),
            file.getName(),
            file.length(),
            getLastModifiedTime(file),
            hash(file),
            new LinkedHashMap<>()
        );

        this.file = file;
        final Optional<Long> unixInode = IOUtils.getUnixInode(file);
        unixInode.ifPresent(it -> addUserDefinedMetadata(SYSTEM_FILE_INODE_META_KEY, it));
        inode = unixInode.orElse(null);
        addUserDefinedMetadata(SYSTEM_FILE_HOSTNAME_META_KEY, Network.HOSTNAME);
    }

    private static long getLastModifiedTime(final File f) {
        try {
            return Files.getLastModifiedTime(f.toPath(), LinkOption.NOFOLLOW_LINKS).to(TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            throw new ConnectFilePulseException(
                "Error while getting last-modified time for file : " + f.getName() + " - " + e.getLocalizedMessage());
        }
    }

    @JsonIgnore
    public String path() {
        return file.getParentFile().getAbsolutePath();
    }

    @JsonIgnore
    public String absolutePath() {
        return file.getAbsolutePath();
    }

    @JsonIgnore
    public Long inode() {
        return inode;
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Creates a CRC32 hash from the first n bytes and the total size of the specified file.
     *
     * @return  a CRC32 hash.
     */
    private static ContentDigest hash(final File f) {
        try {
            CRC32 crc32 = new CRC32();
            if (f.length() > 0) {
                byte[] bytes = readStartingBytesFrom(f, 4096);
                crc32.update(bytes);
                crc32.update(longToBytes(f.length()));
                return new ContentDigest(String.valueOf(crc32.getValue()), "crc32");
            }
            return new ContentDigest("", "crc32");
        } catch (IOException e) {
            throw new ConnectFilePulseException(
                    "Error while computing CRC32 hash for file : " + f.getName() + " - " + e.getLocalizedMessage());
        }
    }

    /**
     * Read the first n bytes of the specified file.
     *
     * @param file  the input file.
     * @param n     the number of bytes to hash.
     * @return      a CRC32 hash.
     *
     * @throws IOException  if error occurred while reading {@code file}.
     */
    private static byte[] readStartingBytesFrom(final File file,
                                                final long n) throws IOException {
        int len = (int) Math.min(file.length(), n);
        byte[] buffer = new byte[len];
        try (InputStream is = new FileInputStream(file)) {
            if (is.read(buffer) != -1) {
                return buffer;
            } else {
                throw new IOException(
                    "Reaching end of the stream while attempting to read first '" +
                    len +
                    "' bytes (file size=" +
                    file.length() + ")."
                );
            }
        }
    }

    private static byte[] longToBytes(final long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

}
