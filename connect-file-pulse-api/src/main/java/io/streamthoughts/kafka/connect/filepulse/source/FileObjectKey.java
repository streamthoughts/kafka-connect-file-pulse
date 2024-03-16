/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Objects;

/**
 * A {@code FileObjectKey} represents a unique ID used for uniquely identifying an object file.
 *
 * @see FileObject
 * @see FileObjectMeta
 * @see SourceOffsetPolicy
 */
public final class FileObjectKey {

    private final String key;

    private final int hashCode;

    public static FileObjectKey of(final String s) {
        return new FileObjectKey(s);
    }

    /**
     * Creates a new {@link FileObjectKey} instance.
     *
     * @param id    the id.
     */
    public FileObjectKey(final String id) {
        this.key = Objects.requireNonNull(id, "'id' should not be null");
        this.hashCode = id.hashCode();
    }

    public String original() {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileObjectKey)) return false;
        FileObjectKey that = (FileObjectKey) o;
        return Objects.equals(key, that.key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return key;
    }
}
