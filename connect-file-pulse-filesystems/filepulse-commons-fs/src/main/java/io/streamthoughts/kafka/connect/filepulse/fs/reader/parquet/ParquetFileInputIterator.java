/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader.parquet;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.ManagedFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.internal.Silent;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.utils.Time;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.*;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileInputIterator extends ManagedFileInputIterator<TypedStruct> {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetFileInputIterator.class);
    private final MessageColumnIO columnIO;
    private final MessageType schema;
    private final ParquetFileReader reader;
    private final long countRows;
    private PageReadStore pages;
    private RecordReader<Group> recordReader;
    private int position;
    private int pageCount;

    /**
     * Creates a new {@link ParquetFileInputIterator} instance.
     *
     * @param objectMeta        The file's metadata.
     * @param iteratorManager   The iterator manager.
     * @param stream         the file's input streams.
     */
    public ParquetFileInputIterator(final FileObjectMeta objectMeta,
                                    final IteratorManager iteratorManager,
                                    final InputStream stream) throws IOException {

        super(objectMeta, iteratorManager);
        this.reader = ParquetFileReader.open(new ParquetInputFile(stream));
        pages = reader.readRowGroup(pageCount);
        countRows = pages.getRowCount() - 1;
        schema = reader.getFooter().getFileMetaData().getSchema();
        columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekTo(final FileObjectOffset offset) {
        Objects.requireNonNull(offset, "offset can't be null");
        if (offset.position() != -1) {
            LOG.info("Seeking to skip to Parquet record {} on page {}", offset.position(), offset.rows());
            position = (int) offset.position() + 1;
            pageCount = (int) offset.rows();
        }
    }

    /**
     * Updates the current {@link #context}.
     */
    private void updateContext() {
        context = context.withOffset(new FileObjectOffset(position, pageCount, Time.SYSTEM.milliseconds()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> next() {
        if (position <= countRows) {
            try {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                var typedStruct = incrementAndGet(ParquetTypedStructConverter.fromParquetFileReader(simpleGroup));
                position++;
                return RecordsIterable.of(typedStruct);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                updateContext();
            }
        }
        return RecordsIterable.empty();
    }

    private FileRecord<TypedStruct> incrementAndGet(final TypedStruct struct) {
        return new TypedFileRecord(new ParquetRecordOffset(position, pageCount), struct);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (position > countRows) {
            try {
                pageCount++;
                pages = reader.readRowGroup(pageCount);
                if (Optional.ofNullable(pages).isEmpty()) {
                    pageCount = 0;
                    return false;
                }
                position = 0;
                recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
            } catch (IOException e) {
                LOG.error("Error while read new RowGroup, {}", e.getMessage());
            }
        }
        return pages != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            if (!isClosed()) {
                Silent.unchecked(reader::close);
                super.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing ParquetFileInputIterator, {}", e.getMessage());
        }
    }
}