/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;


public class DefaultRecordFilterPipelineTest {

    private final FileObjectMeta source = new GenericFileObjectMeta(null, "", 0L, 0L, null, null);
    private final FileObjectContext context = new FileObjectContext(source);


    @Test
    public void testGivenIdentityFilter() {

        FileRecordOffset offset = FileRecordOffset.invalid();
        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(offset, "value1");

        final TestFilter filter = new TestFilter()
                .setFunction(((context, record, hasNext) -> RecordsIterable.of(record)));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(record1), false);

        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(record1, records.last());
    }

    @Test
    public void shouldReThrowExceptionGivenFailingFilterNotIgnoringFailure() {

        FileRecordOffset offset = FileRecordOffset.invalid();
        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(offset, "value1");

        final TestFilter filter = new TestFilter()
            .setException(new RuntimeException("test error"));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter));
        pipeline.init(context);

        try {
            pipeline.apply(new RecordsIterable<>(record1), false);
            fail("expecting error to be thrown");
        } catch (Exception e) {
            assertEquals("test error", e.getMessage());
        }
    }

    @Test
    public void shouldSkipFilterGivenFailingFilterIgnoringFailure() {

        FileRecordOffset offset = FileRecordOffset.invalid();
        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(offset, "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(offset, "value2");

        final TestFilter filter1 = new TestFilter()
            .setException(new RuntimeException("test error"))
            .setIgnoreFailure(true);

        final TestFilter filter2 = new TestFilter()
                .setFunction((o1, o2, o3) -> RecordsIterable.of(record2.value()));

        List<RecordFilter> filters = Arrays.asList(filter1, filter2);
        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(filters);
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(record1), true);

        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(record2, records.last());
    }

    @Test
    public void shouldFlushBufferedRecordsAndSkipFilterGivenFailingFilter() {

        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value2");

        TestFilter filter = new TestFilter()
                .setException(new RuntimeException("test error"))
                .setIgnoreFailure(true)
                .setBuffer(Collections.singletonList(record1));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(record2), true);

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record1, records.collect().get(0));
        assertEquals(record2, records.collect().get(1));
    }

    @Test
    public void shouldFlushBufferedRecordsAndExecuteErrorPipelineGivenFailingFilter() {

        final FileRecord<TypedStruct>  record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        FileRecordOffset offset = FileRecordOffset.invalid();
        final FileRecord<TypedStruct>  record2 = createWithOffsetAndValue(offset, "value2");
        final FileRecord<TypedStruct> record3 = createWithOffsetAndValue(offset, "value2");

        TestFilter filter2 = new TestFilter()
                .setFunction((o1, o2, o3) -> RecordsIterable.of(record3.value()));

        TestFilter filter1 = new TestFilter()
                .setException(new RuntimeException("test error"))
                .setIgnoreFailure(true)
                .setBuffer(Collections.singletonList(record1))
                .setOnFailure(new DefaultRecordFilterPipeline(Collections.singletonList(filter2)));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter1));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(record2), true);

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record1, records.collect().get(0));
        assertEquals(record3, records.collect().get(1));
    }

    @Test
    public void shouldFlushBufferedRecordsGivenNoAcceptFilterAndThereIsNoRemainingRecord() {

        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value2");
        final FileRecord<TypedStruct> record3 = createWithOffsetAndValue(FileRecordOffset.invalid(), "foo");

        TestFilter filter1 = new TestFilter()
                // This function will never be executed
                .setFunction(((context, record, hasNext) -> RecordsIterable.of(record3.value())))
                .setBuffer(Collections.singletonList(record1))
                .refuse();

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter1));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(record2), false);

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record1, records.collect().get(0));
        assertEquals(record2, records.collect().get(1));
    }

    @Test
    public void shouldNotFlushBufferedRecordsGivenNoAcceptFilterAndThereIsNoRemainingRecord() {

        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value2");

        TestFilter filter1 = new TestFilter()
                .setBuffer(Collections.singletonList(record1))
                .refuse();

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter1));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(record2), true);

        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(record2, records.collect().get(0));
    }

    @Test
    public void shouldFlushBufferedRecordsGivenAcceptFilterEmptyRecordsIterableAndNoRemainingRecords() {

        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value2");

        List<FileRecord<TypedStruct>> bufferedRecords = List.of(record1, record2);
        TestFilter filter1 = new TestFilter()
                .setBuffer(bufferedRecords);

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter1));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(), false);

        assertNotNull(records);
        List<FileRecord<TypedStruct>> filteredRecords = records.collect();
        Assertions.assertIterableEquals(bufferedRecords, filteredRecords);
    }

    @Test
    public void shouldFlushBufferedRecordsFromFirstFilterGivenAcceptFiltersEmptyRecordsIterableAndNoRemainingRecods() {

        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value2");

        List<FileRecord<TypedStruct>> bufferedRecords = List.of(record1, record2);
        TestFilter filter1 = new TestFilter()
                .setBuffer(bufferedRecords);
        TestFilter filter2 = new TestFilter()
                .setFunction(((context1, record, hasNext) -> RecordsIterable.of(record)));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(List.of(filter1, filter2));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(), false);

        assertNotNull(records);
        List<FileRecord<TypedStruct>> filteredRecords = records.collect();
        Assertions.assertIterableEquals(bufferedRecords, filteredRecords);
    }

    @Test
    public void shouldFlushBufferedRecordsFromLastFilterGivenAcceptFiltersEmptyRecordsIterableAndNoRemainingRecods() {

        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value2");

        List<FileRecord<TypedStruct>> bufferedRecords = List.of(record1, record2);
        TestFilter filter1 = new TestFilter();
        TestFilter filter2 = new TestFilter()
                .setBuffer(bufferedRecords);
        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(List.of(filter1, filter2));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(), false);

        assertNotNull(records);
        List<FileRecord<TypedStruct>> filteredRecords = records.collect();
        Assertions.assertIterableEquals(bufferedRecords, filteredRecords);
    }

    @Test
    public void shouldFlushBufferedRecordsFromAllFiltersGivenAcceptFiltersEmptyRecordsIterableAndNoRemainingRecods() {

        final FileRecord<TypedStruct> record1 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value1");
        final FileRecord<TypedStruct> record2 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value2");
        final FileRecord<TypedStruct> record3 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value3");
        final FileRecord<TypedStruct> record4 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value4");
        final FileRecord<TypedStruct> record5 = createWithOffsetAndValue(FileRecordOffset.invalid(), "value5");

        List<FileRecord<TypedStruct>> allBuffered = List.of(record1, record2, record3, record4, record5);

        List<FileRecord<TypedStruct>> bufferedRecords1 = List.of(record1);
        List<FileRecord<TypedStruct>> bufferedRecords2 = List.of(record2, record3);
        List<FileRecord<TypedStruct>> bufferedRecords3 = List.of(record4, record5);
        TestFilter filter1 = new TestFilter()
                .setFunction(((context1, record, hasNext) -> RecordsIterable.of(record)))
                .setBuffer(bufferedRecords1);
        TestFilter filter2 = new TestFilter()
                .setFunction(((context1, record, hasNext) -> RecordsIterable.of(record)))
                .setBuffer(bufferedRecords2);
        TestFilter filter3 = new TestFilter()
                .setFunction(((context1, record, hasNext) -> RecordsIterable.of(record)))
                .setBuffer(bufferedRecords3);
        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(List.of(filter1, filter2, filter3));
        pipeline.init(context);

        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(), false);

        assertNotNull(records);
        List<FileRecord<TypedStruct>> filteredRecords = records.collect();
        Assertions.assertIterableEquals(allBuffered, filteredRecords);
    }

    @Test
    public void shouldReturnRecordUnchangedGivenNoFilter() {

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.emptyList());
        pipeline.init(context);

        final FileRecord<TypedStruct> record = createWithOffsetAndValue(FileRecordOffset.invalid(), "value");
        RecordsIterable<FileRecord<TypedStruct>> records = pipeline.apply(new RecordsIterable<>(record), true);
        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(record, records.collect().get(0));
    }

    private static FileRecord<TypedStruct> createWithOffsetAndValue(final FileRecordOffset offset, final String value) {
        return new TypedFileRecord(offset, TypedStruct.create().put("message", value));
    }

    static class TestFilter implements RecordFilter {

        RuntimeException exception;

        RecordFilterPipeline<FileRecord<TypedStruct>> onFailure;

        private boolean ignoreFailure = false;

        private boolean accept = true;

        List<FileRecord<TypedStruct>> buffered;

        private FilterFunction function;

        TestFilter setException(final RuntimeException exception) {
            this.exception = exception;
            return this;
        }

        public boolean accept(final FilterContext context, final TypedStruct record) {
            return accept;
        }

        TestFilter setIgnoreFailure(boolean ignoreFailure) {
            this.ignoreFailure = ignoreFailure;
            return this;
        }

        TestFilter setFunction(final FilterFunction function) {
            this.function = function;
            return this;
        }

        TestFilter setBuffer(final List<FileRecord<TypedStruct>> buffered) {
            this.buffered = buffered;
            return this;
        }

        TestFilter setOnFailure(final RecordFilterPipeline<FileRecord<TypedStruct>> onFailure) {
            this.onFailure = onFailure;
            return this;
        }

        TestFilter refuse() {
            this.accept = false;
            return this;
        }

        @Override
        public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                  final TypedStruct record, boolean hasNext) {
            if (exception != null) {
                throw exception;
            }
            return function.apply(context, record, hasNext);
        }

        @Override
        public RecordsIterable<FileRecord<TypedStruct>> flush() {
            return buffered == null ? RecordsIterable.empty() : new RecordsIterable<>(buffered);
        }

        @Override
        public void configure(Map<String, ?> configs) {

        }

        @Override
        public ConfigDef configDef() {
            return null;
        }

        public RecordFilterPipeline<FileRecord<TypedStruct>> onFailure() {
            return onFailure;
        }

        public boolean ignoreFailure() {
            return ignoreFailure;
        }
    }

    @FunctionalInterface
    interface FilterFunction extends RecordFilter {

        @Override
        default void configure(final Map<String, ?> configs) {

        }

        @Override
        default ConfigDef configDef() {
            return null;
        }
    }
}