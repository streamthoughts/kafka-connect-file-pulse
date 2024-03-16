/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal;

import java.nio.charset.Charset;
import java.util.Objects;

public class TextBlock {

    private final String data;
    private final long startOffset;
    private final long endOffset;
    private final int size;
    private final Charset charset;

    /**
     * Creates a new {@link TextBlock} instance.
     *
     * @param data          the textual value.
     * @param charset       the charset.
     * @param startOffset   the byte starting position of the value.
     * @param endOffset     the byte ending position of the value.
     * @param size          the size of value in bytes
     */
    public TextBlock(final String data,
                     final Charset charset,
                     final long startOffset,
                     final long endOffset,
                     final int size) {
        this.data = data;
        this.charset = charset;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.size = size;
    }

    public String data() {
        return data;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public int size() {
        return size;
    }

    public Charset charset() {
        return charset;
    }

    public byte[] toByteArray() {
        return data.getBytes(charset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TextBlock)) return false;
        TextBlock textBlock = (TextBlock) o;
        return startOffset == textBlock.startOffset &&
                endOffset == textBlock.endOffset &&
                size == textBlock.size &&
                Objects.equals(data, textBlock.data) &&
                Objects.equals(charset, textBlock.charset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(data, startOffset, endOffset, size, charset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "TextBlock{" +
                "value='" + data + '\'' +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", size=" + size +
                ", charset=" + charset +
                '}';
    }
}
