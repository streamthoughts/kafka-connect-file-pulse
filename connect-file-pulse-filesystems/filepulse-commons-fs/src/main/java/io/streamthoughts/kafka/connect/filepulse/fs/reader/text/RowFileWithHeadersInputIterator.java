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
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal.TextBlock;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowFileWithHeadersInputIterator extends RowFileInputIteratorDecorator {

    private static final Logger LOG = LoggerFactory.getLogger(RowFileWithHeadersInputIterator.class);

    private static final String HEADERS_RECORD_FIELD = "headers";

    /**
     * The number of rows to be skipped in the beginning of file.
     */
    private final int skipHeaders;

    private final Supplier<NonBlockingBufferReader> reader;

    private List<TextBlock> headers;

    private List<String> headerStrings;

    public RowFileWithHeadersInputIterator(final int skipHeaders,
                                           final Supplier<NonBlockingBufferReader> reader,
                                           final FileInputIterator<FileRecord<TypedStruct>> iterator) {
        super(iterator);
        this.skipHeaders = skipHeaders;
        this.reader = reader;
    }

    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        if (headers == null) {
            LOG.info("Starting to read header lines ({}) from file {}", skipHeaders, context().metadata().uri());
            try (final NonBlockingBufferReader sequentialReader = reader.get()) {
                headers = sequentialReader.readLines(skipHeaders, true);
                headerStrings = headers
                        .stream()
                        .map(TextBlock::data)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("", e);
            }
            if (headers.size() < skipHeaders) {
                throw new ReaderException(
                        String.format(
                                "Not enough data for reading headers from file %s (available=%d, expecting=%d)",
                                context().metadata().uri(),
                                headers.size(),
                                skipHeaders)
                );
            }
        }

        final RecordsIterable<FileRecord<TypedStruct>> records = iterator.next();

        return new RecordsIterable<>(records.stream()
                .filter(isNotHeaderLine()).peek(record -> record.value().put(HEADERS_RECORD_FIELD, headerStrings))
                .collect(Collectors.toList()));
    }

    private Predicate<FileRecord<TypedStruct>> isNotHeaderLine() {
        return record -> {
            final RowFileRecordOffset offset = (RowFileRecordOffset) record.offset();
            return offset.startPosition() > headers.get(skipHeaders - 1).startOffset();
        };
    }
}
