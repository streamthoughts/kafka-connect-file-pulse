/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

/**
 * ReversedInputFileReader is attended to be used to read a fixed number of lines from bottom of a file.
 * This class is not optimized for reading a whole file from bottom to top.
 */
public class ReversedInputFileReader implements AutoCloseable {

    static final int DEFAULT_INITIAL_CAPACITY = 4096;

    private static final String READ_MODE = "r";

    private static final String EMPTY_LINE = "";

    private static final int LF = 0x0A;
    private static final int CR = 0x0D;

    private final RandomAccessFile input;

    // The current file-pointer startPosition.
    private long position;

    // The current bytes startPosition.
    private long offset;

    private int bufferEndOffset;

    private final long length;

    private final Charset charset;

    // The buffer used to read extract lines from the iterator file.
    private byte[] buffer;

    // The current startPosition buffer (note buffer is read from
    // end position (buffer.length) to init position 0).
    private int bufferOffset;

    // The number of bytes unread in the buffer.
    private int remaining;

    /**
     * Creates a new {@link ReversedInputFileReader} instance.
     *
     * @param path              the file path.
     * @param charset           the file charset.
     *
     * @throws IOException      if the specified input path is invalid.
     */
    public ReversedInputFileReader(final String path,
                                   final Charset charset) throws IOException {
        this(path, DEFAULT_INITIAL_CAPACITY, charset);
    }

    /**
     * Creates a new {@link ReversedInputFileReader} instance.
     *
     * @param path                  the file path.
     * @param initialCapacity       the buffer initial capacity.
     * @param charset               the file charset.
     *
     * @throws IOException          if the specified input path is invalid.
     */
    ReversedInputFileReader(final String path,
                            final int initialCapacity,
                            final Charset charset) throws IOException {
        final File inputFile = new File(path);
        this.length = inputFile.length();
        this.charset = charset;
        this.input = new RandomAccessFile(path, READ_MODE);

        this.buffer = new byte[initialCapacity > length ? (int)length : initialCapacity];

        this.position = length;
        this.bufferOffset = bufferLength();
        this.remaining = this.bufferOffset;
        this.offset = position;
    }

    private void seekTo(final long offset) throws IOException {
        if (isInvalidOffset(offset)) {
            throw new IllegalArgumentException("Invalid Offset, can't seek to startPosition " + offset);
        }
        position = offset;
        input.seek(offset);
    }

    private boolean isInvalidOffset(long offset) {
        return offset < 0 || offset > length;
    }

    public List<TextBlock> readLines(int minRecords) throws IOException {

        if (isInvalidOffset(position)) {
            return null;
        }

        List<TextBlock> records = new LinkedList<>();
        while (hasNext() && (records.isEmpty() || records.size() < minRecords)) {
            int nread = bufferOffset;
            long nextPosition = Math.max(0, position - nread);

            if (nextPosition == 0 && bufferLength() > position) {
                resizeBuffer((int) position + remaining);
                nread = (int) position + 1;
                remaining = bufferLength();
            }
            bufferEndOffset = bufferLength() - 1;
            seekTo(nextPosition);

            input.readFully(buffer, 0, nread);

            // Reset buffer cursor to beginning before reading.
            bufferOffset = 0;
            TextBlock line;
            do {
                line = tryToExtractLine();
                // We exclude all empty row
                if (line != null && line.size() > 0) {
                    records.add(line);
                }
            } while (line != null && records.size() < minRecords);

            // we didn't find new row after reading fully buffer
            if (records.isEmpty() && bufferOffset == 0) {
                final int prevSize = bufferLength();
                resizeBuffer(bufferLength() * 2);
                this.bufferOffset = bufferLength() - prevSize;
            }
        }
        return records;
    }

    private int bufferLength() {
        return buffer.length;
    }

    private void resizeBuffer(final int size) {
        byte[] newbuf = new byte[size];
        int length = bufferLength() - bufferOffset;
        System.arraycopy(buffer, bufferOffset, newbuf, size - length, length);
        buffer = newbuf;
    }

    private TextBlock tryToExtractLine() {
        int newline = -1;
        int newStart = -1;
        // We always read buffer from the end
        for (int i = bufferEndOffset; i > bufferOffset; i--) {
            // LF
            if (buffer[i] == LF) {
                newline = i;
                newStart = i - 1;
                // We need to check for \r\n, so we must skip this if we can't check the previous char
                if (i - 1 >= bufferOffset) {
                    newStart = (buffer[i - 1] == CR) ? i - 2 : i - 1;
                }
                break;
            }
        }

        if (newline != -1) {
            final int nread = bufferEndOffset - newline;
            String line = nread > 0 ? new String(buffer, newline + 1, nread, charset) : EMPTY_LINE;
            TextBlock result = new TextBlock(line, charset, offset - nread, offset, nread);

            remaining = newStart - bufferOffset + 1;
            System.arraycopy(buffer, bufferOffset, buffer, bufferLength() - remaining, remaining);
            bufferOffset = bufferLength() - remaining;
            offset -= bufferEndOffset - newStart;
            return result;
        } else if (position == 0 && remaining > 0) {
            String line = new String(buffer, bufferOffset, remaining, charset);
            TextBlock result = new TextBlock(line, charset, 0L, remaining, remaining);
            offset = remaining = 0;
            bufferOffset = bufferLength();
            return result;
        } else {
            return null;
        }
    }

    boolean hasNext() {
        return position > 0 || remaining > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        if (this.input != null) {
            input.close();
        }
    }
}