/*
 * Copyright 2019 StreamThoughts.
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

import java.util.Objects;

public abstract class AbstractFileRecord<T> implements FileRecord<T> {

    private final T value;
    private final FileRecordOffset offset;

    /**
     * Creates a new {@link AbstractFileRecord} instance.
     * @param offset    the {@link FileRecordOffset} instance.
     * @param value     the record value.
     */
    AbstractFileRecord(final FileRecordOffset offset,
                       final T value) {
        Objects.requireNonNull(offset, "offset cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        this.value = value;
        this.offset = offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T value() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileRecordOffset offset() {
        return offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractFileRecord)) return false;
        AbstractFileRecord<?> that = (AbstractFileRecord<?>) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(offset, that.offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(value, offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "value=" + value +
                ", offset=" + offset +
                "]";
    }
}
