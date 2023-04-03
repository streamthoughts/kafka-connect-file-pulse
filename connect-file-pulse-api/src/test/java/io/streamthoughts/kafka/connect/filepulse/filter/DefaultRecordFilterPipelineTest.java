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