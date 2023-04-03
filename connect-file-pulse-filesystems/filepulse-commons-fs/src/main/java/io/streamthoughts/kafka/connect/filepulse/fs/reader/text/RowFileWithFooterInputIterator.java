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
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal.ReversedInputFileReader;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal.TextBlock;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.io.File;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowFileWithFooterInputIterator extends RowFileInputIteratorDecorator {

    private static final Logger LOG = LoggerFactory.getLogger(RowFileWithFooterInputIterator.class);

    private static final String HEADERS_RECORD_FIELD = "footers";

    /**
     * The number of rows to be skipped at the end of file.
     */
    private final int skipFooters;

    /**
     * The local file.
     */
    private final File file;

    /**
     * The file charset.
     */
    private final Charset charset;

    private List<TextBlock> footers;

    private List<String> footersStrings;

    public RowFileWithFooterInputIterator(final int skipFooters,
                                          final URI uri,
                                          final Charset charset,
                                          final FileInputIterator<FileRecord<TypedStruct>> iterator) {
        super(iterator);
        this.skipFooters = skipFooters;
        this.file = new File(uri);
        this.charset = charset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        if (footers == null) {
            final String path = file.getPath();
            if (skipFooters > 0) {
                LOG.info("Starting to read footer lines ({}) from file {}", skipFooters, file.getName());

                try (final ReversedInputFileReader reversedReader = new ReversedInputFileReader(path, charset)) {
                    footers = reversedReader.readLines(skipFooters);
                } catch (Exception e) {
                    throw new RuntimeException("", e);
                }
                if (footers.size() < skipFooters) {
                    throw new ReaderException("Not enough data for reading footers from file "
                            + path
                            + " (available=" + footers.size() + ", expecting=" + skipFooters + ")");
                }
                Collections.reverse(footers);
                footersStrings = footers.stream().map(TextBlock::data).collect(Collectors.toList());
            }
        }

        final RecordsIterable<FileRecord<TypedStruct>> records = iterator.next();

        return new RecordsIterable<>(records.stream()
                .filter(isNotFooterLine())
                .peek(record -> record.value().put(HEADERS_RECORD_FIELD, footersStrings))
                .collect(Collectors.toList()));
    }

    private Predicate<FileRecord<TypedStruct>> isNotFooterLine() {
        return record -> ((RowFileRecordOffset) record.offset()).startPosition() < footers.get(0).startOffset();
    }
}
