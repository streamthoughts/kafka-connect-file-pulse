/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.NonBlockingBufferReader;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorBuilder;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputReaderConfig;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * The {@code RowFileInputReader} reads an input local file line by line.
 */
public class LocalRowFileInputReader extends BaseLocalFileInputReader {

    private RowFileInputReaderConfig configs;

    /**
     * Creates a new {@link LocalRowFileInputReader} instance.
     */
    public LocalRowFileInputReader() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.configs = new RowFileInputReaderConfig(configs);
    }

    public RowFileInputReaderConfig config() {
        return configs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileContext context,
                                                                     final IteratorManager iteratorManager) {
        return new RowFileInputIteratorBuilder()
                .withContext(context)
                .withCharset(configs.charset())
                .withMinNumReadRecords(configs.minReadRecords())
                .withSkipHeaders(configs.skipHeaders())
                .withSkipFooters(configs.skipFooters())
                .withMaxWaitMs(configs.maxWaitMs())
                .withIteratorManager(iteratorManager)
                .withReaderSupplier(() -> {
                    try {
                        final File file = new File(context.metadata().uri());
                        final FileInputStream stream = new FileInputStream(file);
                        var br = new NonBlockingBufferReader(
                                stream,
                                configs.bufferInitialBytesSize(),
                                configs.charset()
                        );
                        br.disableAutoFlush();
                        return br;
                    } catch (FileNotFoundException e) {
                        throw new ReaderException("Failed to open file: " + context.metadata().uri());
                    }
                })
                .build();
    }
}
