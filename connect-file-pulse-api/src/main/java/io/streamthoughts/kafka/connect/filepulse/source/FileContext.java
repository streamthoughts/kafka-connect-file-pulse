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

import java.io.File;
import java.util.Objects;

/**
 * Immutable class which is use to wrap contextual information about an input file.
 */
public class FileContext {

    private final SourceMetadata metadata;

    private final SourceOffset offset;

    /**
     * Creates a new {@link FileContext} instance.
     *
     * @param metadata  the source metadata.
     */
    public FileContext(final SourceMetadata metadata) {
        this(metadata, SourceOffset.empty());
    }

    /**
     * Creates a new {@link FileContext} instance.
     *
     * @param metadata  the source metadata.
     * @param offset    teh source startPosition.
     */
    public FileContext(final SourceMetadata metadata,
                       final SourceOffset offset) {
        Objects.requireNonNull(metadata, "metadata can't be null");
        Objects.requireNonNull(offset, "startPosition can't be null");
        this.metadata = metadata;
        this.offset = offset;
    }

    /**
     * Returns this file.
     *
     * @return a new {@link File} instance.
     */
    public File file() {
        return new File(metadata.absolutePath());
    }

    /**
     * Returns the metadata for this file.
     *
     * @return the {@link SourceMetadata} instance.
     */
    public SourceMetadata metadata() {
        return metadata;
    }

    /**
     * Returns the startPosition of the next bytes to read in this file.
     *
     * @return the {@link FileRecordOffset} instance.
     */
    public SourceOffset offset() {
        return offset;
    }
    
    public FileContext withOffset(final SourceOffset offset) {
        return new FileContext(metadata, offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileContext)) return false;
        FileContext that = (FileContext) o;
        return Objects.equals(metadata, that.metadata) &&
                Objects.equals(offset, that.offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(metadata, offset);
    }

    @Override
    public String toString() {
        return "[" +
                "metadata=" + metadata +
                ", offset=" + offset +
                ']';
    }
}