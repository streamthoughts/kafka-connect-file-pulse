/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Objects;

/**
 * Immutable class which is used to wrap contextual information about an input file.
 */
public final class FileObjectContext {

    private final FileObjectKey key;

    private final FileObjectMeta metadata;

    private final FileObjectOffset offset;

    /**
     * Creates a new {@link FileObjectContext} instance.
     *
     * @param metadata  the source metadata.
     */
    public FileObjectContext(final FileObjectMeta metadata) {
        this(null, metadata);
    }

    /**
     * Creates a new {@link FileObjectContext} instance.
     *
     * @param metadata  the source metadata.
     */
    public FileObjectContext(final FileObjectKey key, final FileObjectMeta metadata) {
        this(key, metadata, FileObjectOffset.empty());
    }


    /**
     * Creates a new {@link FileObjectContext} instance.
     *
     * @param key       the object file's key.
     * @param metadata  the object file's metadata.
     * @param offset    the object file's startPosition.
     */
    public FileObjectContext(final FileObjectKey key,
                             final FileObjectMeta metadata,
                             final FileObjectOffset offset) {
        this.metadata =  Objects.requireNonNull(metadata, "metadata can't be null");
        this.offset = Objects.requireNonNull(offset, "startPosition can't be null");
        this.key = key;
    }

    /**
     * Returns the metadata for this file.
     *
     * @return the {@link FileObjectMeta} instance.
     */
    public FileObjectMeta metadata() {
        return metadata;
    }

    /**
     * Returns the startPosition of the next bytes to read in this file.
     *
     * @return the {@link FileRecordOffset} instance.
     */
    public FileObjectOffset offset() {
        return offset;
    }

    /**
     * Returns the partition string identifier for this file.
     *
     * @return the partition.
     */
    public FileObjectKey key() {
        return key;
    }

    public FileObjectContext withOffset(final FileObjectOffset offset) {
        return new FileObjectContext(key, metadata, offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileObjectContext)) return false;
        FileObjectContext that = (FileObjectContext) o;
        return Objects.equals(key, that.key) &&
               Objects.equals(metadata, that.metadata) &&
                Objects.equals(offset, that.offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(key, metadata, offset);
    }

    @Override
    public String toString() {
        return "[" +
                "partition=" + key +
                ", metadata="  + metadata +
                ", offset="  + offset +
                ']';
    }
}