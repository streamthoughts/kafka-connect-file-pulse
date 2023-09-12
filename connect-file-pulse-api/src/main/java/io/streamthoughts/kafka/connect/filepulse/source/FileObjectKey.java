/*
 * Copyright 2021 StreamThoughts.
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
