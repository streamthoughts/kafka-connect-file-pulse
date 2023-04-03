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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.text;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class to build a new {@link RowFileInputIterator}.
 */
public class RowFileInputIteratorBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(RowFileInputIteratorBuilder.class);

    private Charset charset = StandardCharsets.UTF_8;
    private int minNumReadRecords = 1;
    private FileObjectMeta metadata;
    private long waitMaxMs = 0;
    private int skipHeaders = 0;
    private int skipFooters = 0;
    private IteratorManager iteratorManager;
    private Supplier<NonBlockingBufferReader> readerSupplier;

    public RowFileInputIteratorBuilder withReaderSupplier(final Supplier<NonBlockingBufferReader> readerSupplier) {
        this.readerSupplier = readerSupplier;
        return this;
    }

    public RowFileInputIteratorBuilder withMetadata(final FileObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public RowFileInputIteratorBuilder withSkipHeaders(final int skipHeaders) {
        this.skipHeaders = skipHeaders;
        return this;
    }

    public RowFileInputIteratorBuilder withSkipFooters(final int skipFooters) {
        this.skipFooters = skipFooters;
        return this;
    }

    public RowFileInputIteratorBuilder withMinNumReadRecords(final int minNumReadRecords) {
        this.minNumReadRecords = minNumReadRecords;
        return this;
    }

    public RowFileInputIteratorBuilder withMaxWaitMs(final long maxWaitMs) {
        this.waitMaxMs = maxWaitMs;
        return this;
    }

    public RowFileInputIteratorBuilder withCharset(final Charset charset) {
        this.charset = charset;
        return this;
    }

    public RowFileInputIteratorBuilder withIteratorManager(final IteratorManager iteratorManager) {
        this.iteratorManager = iteratorManager;
        return this;
    }

    public FileInputIterator<FileRecord<TypedStruct>> build() {
        FileInputIterator<FileRecord<TypedStruct>> iterator;

        iterator = new RowFileInputIterator(metadata, iteratorManager, readerSupplier.get())
                .setMinNumReadRecords(minNumReadRecords)
                .setMaxWaitMs(waitMaxMs);

        if (skipFooters > 0) {
            LOG.debug("Decorate RowFileInputIterator with RowFileWithFooterInputIterator");
            iterator = new RowFileWithFooterInputIterator(
                skipFooters,
                metadata.uri(),
                charset,
                iterator
            );
        }

        if (skipHeaders > 0) {
            LOG.debug("Decorate RowFileInputIterator with RowFileWithHeadersInputIterator");
            iterator = new RowFileWithHeadersInputIterator(
                skipHeaders,
                readerSupplier,
                iterator
            );
        }

        return iterator;
    }
}
