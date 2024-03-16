/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.iterator;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.NonBlockingBufferReader;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorConfig;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;

public class SftpRowFileInputIteratorFactory implements FileInputIteratorFactory {

    private final RowFileInputIteratorConfig configs;

    private final Storage storage;

    private final IteratorManager iteratorManager;

    public SftpRowFileInputIteratorFactory(final RowFileInputIteratorConfig config,
                                           final Storage storage,
                                           final IteratorManager iteratorManager) {
        this.configs = config;
        this.storage = storage;
        this.iteratorManager = iteratorManager;
    }

    @Override
    public FileInputIterator<FileRecord<TypedStruct>> newIterator(URI objectURI) {
        final FileObjectMeta objectMetadata = storage.getObjectMetadata(objectURI);

        return new SftpRowFileInputIteratorBuilder()
                .withMetadata(objectMetadata)
                .withCharset(configs.charset())
                .withMinNumReadRecords(configs.minReadRecords())
                .withSkipHeaders(configs.skipHeaders())
                .withSkipFooters(configs.skipFooters())
                .withMaxWaitMs(configs.maxWaitMs())
                .withIteratorManager(iteratorManager)
                .withReader(tryConfigureReader(objectURI))
                .build();
    }

    private NonBlockingBufferReader tryConfigureReader(URI objectURI) {
        try {
            return configureReader(objectURI);
        } catch (Exception e) {
            throw new ReaderException("Cannot get InputStream", e);
        }
    }

    private NonBlockingBufferReader configureReader(URI objectURI) throws Exception {
        NonBlockingBufferReader br = new NonBlockingBufferReader(
                storage.getInputStream(objectURI),
                configs.bufferInitialBytesSize(),
                configs.charset()
        );

        br.disableAutoFlush();
        return br;
    }
}
