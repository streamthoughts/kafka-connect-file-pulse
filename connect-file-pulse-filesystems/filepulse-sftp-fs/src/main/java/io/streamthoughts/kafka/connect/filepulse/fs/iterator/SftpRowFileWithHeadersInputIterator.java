/*
 * Copyright 2019-2023 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.iterator;

import static java.util.stream.Collectors.toList;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.NonBlockingBufferReader;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorDecorator;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileWithHeadersInputIterator;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.internal.TextBlock;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpRowFileWithHeadersInputIterator extends RowFileInputIteratorDecorator {

    private static final Logger log = LoggerFactory.getLogger(SftpRowFileWithHeadersInputIterator.class);
    private static final String HEADERS_RECORD_FIELD = "headers";

    /**
     * The number of rows to be skipped at the beginning of file.
     */
    private final int skipHeaders;

    private final NonBlockingBufferReader sequentialReader;

    private List<TextBlock> headers;

    private List<String> headerNames;

    public SftpRowFileWithHeadersInputIterator(final int skipHeaders,
                                               final NonBlockingBufferReader sequentialReader,
                                               final FileInputIterator<FileRecord<TypedStruct>> iterator) {
        super(iterator);
        this.skipHeaders = skipHeaders;
        this.sequentialReader = sequentialReader;
    }

    /** We had to reimplement this class since
     * @see RowFileWithHeadersInputIterator#next() closes the stream after reading the header in the try-with-resources.
     * When reading a file from the remote sftp server we want to handle one single flow of reads.
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        if (headers == null) {
            initHeaders();
        }

        final RecordsIterable<FileRecord<TypedStruct>> records = iterator.next();

        return new RecordsIterable<>(records.stream()
                .filter(isNotHeaderLine())
                .peek(record -> record.value().put(HEADERS_RECORD_FIELD, headerNames))
                .collect(toList()));
    }

    void initHeaders() {
        log.info("Starting to read header lines ({}) from file {}", skipHeaders, context().metadata().uri());

        try {
            headers = sequentialReader.readLines(skipHeaders, true);
            headerNames = headers
                    .stream()
                    .map(TextBlock::data)
                    .collect(toList());

            log.info("Retrieved headers '{}'", headerNames);
        } catch (IOException e) {
            throw new ReaderException(String.format("Cannot read lines from file %s",
                    context().metadata().uri()), e);
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

    private Predicate<FileRecord<TypedStruct>> isNotHeaderLine() {
        return record -> {
            final RowFileRecordOffset offset = (RowFileRecordOffset) record.offset();
            return offset.startPosition() > headers.get(skipHeaders - 1).startOffset();
        };
    }

    List<String> getHeaderNames() {
        return headerNames;
    }
}
