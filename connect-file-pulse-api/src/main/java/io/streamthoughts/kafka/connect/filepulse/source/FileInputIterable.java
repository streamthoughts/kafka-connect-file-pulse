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

import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileInputIterable implements Iterable<RecordsIterable<FileInputRecord>> {

    private static final Logger LOG = LoggerFactory.getLogger(FileInputIterable.class);

    private final File file;
    private final FileInputReader reader;
    private SourceMetadata metadata;
    private FileInputIterator<FileInputRecord> iterator;

    private AtomicBoolean isOpen = new AtomicBoolean(false);

    /**
     * Creates a new {@link FileInputIterable} instance.
     *
     * @param source the input source file.
     * @param reader the input source reader used to create a new {@link FileInputIterator}.
     */
    FileInputIterable(final File source, final FileInputReader reader) {
        Objects.requireNonNull(source, "source can't be null");
        Objects.requireNonNull(reader, "reader can't be null");
        this.file = source;
        this.reader = reader;
        this.metadata = SourceMetadata.fromFile(source);
    }

    /**
     * Opens a new iterator.
     *
     * @param offset    the offset to seek the iterator.
     * @return a new {@link FileInputIterator} instance.
     */
    public FileInputIterator<FileInputRecord> open(final SourceOffset offset) {
        LOG.info("Opening new iterator for source : {}", metadata);
        iterator = reader.newIterator(new FileInputContext(metadata));
        iterator.seekTo(offset);
        isOpen.set(true);
        return iterator;
    }

    public SourceMetadata metadata() {
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileInputIterator<FileInputRecord> iterator() {
        checkState();
        return iterator;
    }

    boolean isOpen() {
        return isOpen.get();
    }

    boolean isValid() {
        return file.exists() && file.canRead();
    }

    public File file() {
        return file;
    }

    private void checkState() {
        if (!isOpen()) {
            throw new IllegalStateException("iterator is not open");
        }
    }

    void close() {
        if (isOpen.get()) {
            LOG.debug("Closing file {} ", file.getAbsolutePath());
            iterator.close();
        }
    }

    static boolean isAlreadyCompleted(final SourceOffset committedOffset,
                                      final SourceMetadata metadata) {
        return committedOffset != null &&
                committedOffset.position() >= metadata.size();
    }

}