/*
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

import com.jsoniter.annotation.JsonCreator;
import com.jsoniter.annotation.JsonProperty;
import io.streamthoughts.kafka.connect.filepulse.internal.Network;
import io.streamthoughts.kafka.connect.filepulse.internal.IOUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Immutable class used to wrap all input source file metadata.
 */
public class SourceMetadata implements Comparable<SourceMetadata> {

    private final String name;
    private final String path;
    private final long size;
    private final long lastModified;
    private final Long inode;
    private final long hash;
    private transient ConnectHeaders headers = null;

    /**
     * Creates a new {@link SourceMetadata} for the specified file.
     *
     * @param file a source file.
     * @return a new {@link SourceMetadata} instance.
     */
    public static SourceMetadata fromFile(final File file) {
        Objects.requireNonNull(file);
        try {
            long hash = hash(file);
            return new SourceMetadata(
                file.getName(),
                file.getParentFile().getAbsolutePath(),
                file.length(),
                file.lastModified(),
                IOUtils.getUnixInode(file).orElse(null),
                hash);
        } catch (IOException e) {
            throw new ConnectException(
                "Error while computing CRC32 hash for file : " + file.getName() + " - " + e.getLocalizedMessage());
        }
    }

    /**
     * Creates a new {@link SourceMetadata} instance.
     *
     * @param fileName          the name of source file.
     * @param filePath          the path of source file.
     * @param fileSize          the size of source file.
     * @param fileLastModified  the file last modified time.
     * @param inode             the unix inode attached to the file.
     * @param hash              the hash of the source content file.
     */
    @JsonCreator
    public SourceMetadata(@JsonProperty("name") final String fileName,
                          @JsonProperty("path") final String filePath,
                          @JsonProperty("size") final long fileSize,
                          @JsonProperty("lastModified") final long fileLastModified,
                          @JsonProperty("inode") final Long inode,
                          @JsonProperty("hash") final long hash) {
        this.name = fileName;
        this.path = filePath;
        this.size = fileSize;
        this.lastModified = fileLastModified;
        this.inode = inode;
        this.hash = hash;
    }

    public String name() {
        return name;
    }

    public String path() {
        return path;
    }

    public long size() {
        return size;
    }

    public long lastModified() {
        return lastModified;
    }

    public Long inode() {
        return inode;
    }

    public long hash() {
        return hash;
    }

    public String absolutePath() {
        return new File(path, name).getAbsolutePath();
    }

    public ConnectHeaders toConnectHeader() {
        if (headers == null) {
            headers = new ConnectHeaders();
            headers.addString("connect.file.name", name);
            headers.addString("connect.file.path", path);
            headers.addLong("connect.file.hash", hash);
            headers.addLong("connect.file.size", size);
            headers.addLong("connect.file.lastModified", lastModified);
            headers.addString("connect.hostname", Network.HOSTNAME);
        }
        return headers;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceMetadata)) return false;
        SourceMetadata that = (SourceMetadata) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(path, that.path) &&
               Objects.equals(inode, that.inode) &&
               Objects.equals(hash, that.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path, inode, hash);
    }

    @Override
    public String toString() {
        return "[" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", size=" + size +
                ", lastModified=" + lastModified +
                ", inode=" + inode +
                ", hash=" + hash +
                ']';
    }

    /**
     * Creates a CRC32 hash from the first n bytes and the total size of the specified file.
     *
     * @return  a CRC32 hash.
     * @throws IOException  if error occurred while reading {@code file}.
     */
    private static long hash(final File f) throws IOException {
        CRC32 crc32 = new CRC32();
        if (f.length() > 0) {
            byte[] bytes = readStartingBytesFrom(f, 4096);
            crc32.update(bytes);
            crc32.update(longToBytes(f.length()));
            return crc32.getValue();
        }
        return -1;
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

    @Override
    public int compareTo(final SourceMetadata that) {
        return Long.compare(this.lastModified, that.lastModified);
    }
}
