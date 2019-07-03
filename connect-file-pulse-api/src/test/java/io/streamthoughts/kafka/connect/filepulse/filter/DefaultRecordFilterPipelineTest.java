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
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class DefaultRecordFilterPipelineTest {

    private SourceMetadata metadata = new SourceMetadata("", "", 0L, 0L, 0L, -1L);
    private FileInputContext context = new FileInputContext(metadata);

    @Test
    public void testGivenIdentityFilter() {

        FileInputOffset offset = FileInputOffset.with(1, 0);
        final FileInputRecord record1 = createWithOffsetAndValue(offset, "value1");

        final TestFilter filter = new TestFilter()
                .setFunction(((context, record, hasNext) -> RecordsIterable.of(record)));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> records = pipeline.apply(new RecordsIterable<>(record1), false);

        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(record1, records.last());
    }

    @Test
    public void shouldReThrowExceptionGivenFailingFilterNotIgnoringFailure() {

        FileInputOffset offset = FileInputOffset.with(1, 0);
        final FileInputRecord record1 = createWithOffsetAndValue(offset, "value1");

        final TestFilter filter = new TestFilter()
            .setException(new RuntimeException("test exception"));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter));
        pipeline.init(context);

        try {
            pipeline.apply(new RecordsIterable<>(record1), false);
            fail("expecting exception to be thrown");
        } catch (Exception e) {
            assertEquals("test exception", e.getMessage());
        }
    }

    @Test
    public void shouldSkipFilterGivenFailingFilterIgnoringFailure() {

        FileInputOffset offset = FileInputOffset.with(1, 0);
        final FileInputRecord record1 = createWithOffsetAndValue(offset, "value1");
        final FileInputRecord record2 = createWithOffsetAndValue(offset, "value2");

        final TestFilter filter1 = new TestFilter()
            .setException(new RuntimeException("test exception"))
            .setIgnoreFailure(true);

        final TestFilter filter2 = new TestFilter()
                .setFunction((o1, o2, o3) -> RecordsIterable.of(record2.data()));

        List<RecordFilter> filters = Arrays.asList(filter1, filter2);
        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(filters);
        pipeline.init(context);

        RecordsIterable<FileInputRecord> records = pipeline.apply(new RecordsIterable<>(record1), true);

        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(record2, records.last());
    }

    @Test
    public void shouldFlushBufferedRecordsAndSkipFilterGivenFailingFilter() {

        final FileInputRecord record1 = createWithOffsetAndValue(FileInputOffset.with(1, 0), "value1");
        final FileInputRecord record2 = createWithOffsetAndValue(FileInputOffset.with(2, 0), "value2");

        TestFilter filter = new TestFilter()
                .setException(new RuntimeException("test exception"))
                .setIgnoreFailure(true)
                .setBuffer(Collections.singletonList(record1));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> records = pipeline.apply(new RecordsIterable<>(record2), true);

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record1, records.collect().get(0));
        assertEquals(record2, records.collect().get(1));
    }

    @Test
    public void shouldFlushBufferedRecordsAndExecuteErrorPipelineGivenFailingFilter() {

        final FileInputRecord record1 = createWithOffsetAndValue(FileInputOffset.with(1, 0), "value1");
        FileInputOffset offset = FileInputOffset.with(2, 0);
        final FileInputRecord record2 = createWithOffsetAndValue(offset, "value2");
        final FileInputRecord record3 = createWithOffsetAndValue(offset, "value2");

        TestFilter filter2 = new TestFilter()
                .setFunction((o1, o2, o3) -> RecordsIterable.of(record3.data()));

        TestFilter filter1 = new TestFilter()
                .setException(new RuntimeException("test exception"))
                .setIgnoreFailure(true)
                .setBuffer(Collections.singletonList(record1))
                .setOnFailure(new DefaultRecordFilterPipeline(Collections.singletonList(filter2)));

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter1));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> records = pipeline.apply(new RecordsIterable<>(record2), true);

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record1, records.collect().get(0));
        assertEquals(record3, records.collect().get(1));
    }

    @Test
    public void shouldFlushBufferedRecordsGivenNoAcceptFilterAndThereIsNoRemainingRecord() {

        final FileInputRecord record1 = createWithOffsetAndValue(FileInputOffset.with(1, 0), "value1");
        final FileInputRecord record2 = createWithOffsetAndValue(FileInputOffset.with(2, 0), "value2");
        final FileInputRecord record3 = createWithOffsetAndValue(FileInputOffset.with(2, 0), "foo");

        TestFilter filter1 = new TestFilter()
                // This function will never be executed
                .setFunction(((context, record, hasNext) -> RecordsIterable.of(record3.data())))
                .setBuffer(Collections.singletonList(record1))
                .refuse();

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter1));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> records = pipeline.apply(new RecordsIterable<>(record2), false);

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record1, records.collect().get(0));
        assertEquals(record2, records.collect().get(1));
    }

    @Test
    public void shouldNotFlushBufferedRecordsGivenNoAcceptFilterAndThereIsNoRemainingRecord() {

        final FileInputRecord record1 = createWithOffsetAndValue(FileInputOffset.with(1, 0), "value1");
        final FileInputRecord record2 = createWithOffsetAndValue(FileInputOffset.with(2, 0), "value2");

        TestFilter filter1 = new TestFilter()
                .setBuffer(Collections.singletonList(record1))
                .refuse();

        DefaultRecordFilterPipeline pipeline = new DefaultRecordFilterPipeline(Collections.singletonList(filter1));
        pipeline.init(context);

        RecordsIterable<FileInputRecord> records = pipeline.apply(new RecordsIterable<>(record2), true);

        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(record2, records.collect().get(0));
    }

    private static FileInputRecord createWithOffsetAndValue(final FileInputOffset offset, final String value) {
        return new FileInputRecord(offset, FileInputData.defaultStruct(value));
    }

    static class TestFilter implements RecordFilter {

        RuntimeException exception;

        RecordFilterPipeline<FileInputRecord> onFailure;

        private boolean ignoreFailure = false;

        private boolean accept = true;

        List<FileInputRecord> buffered;

        private FilterFunction function;

        TestFilter setException(final RuntimeException exception) {
            this.exception = exception;
            return this;
        }

        TestFilter setIgnoreFailure(boolean ignoreFailure) {
            this.ignoreFailure = ignoreFailure;
            return this;
        }

        TestFilter setFunction(final FilterFunction function) {
            this.function = function;
            return this;
        }

        TestFilter setBuffer(final List<FileInputRecord> buffered) {
            this.buffered = buffered;
            return this;
        }

        TestFilter setOnFailure(final RecordFilterPipeline<FileInputRecord> onFailure) {
            this.onFailure = onFailure;
            return this;
        }

        TestFilter refuse() {
            this.accept = false;
            return this;
        }

        @Override
        public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                    final FileInputData record, boolean hasNext) {
            if (exception != null) {
                throw exception;
            }
            return function.apply(context, record, hasNext);
        }

        @Override
        public RecordsIterable<FileInputRecord> flush() {
            return buffered == null ? RecordsIterable.empty() : new RecordsIterable<>(buffered);
        }

        @Override
        public void configure(Map<String, ?> configs) {

        }

        public boolean accept(final FilterContext context, final FileInputData record) {
            return accept;
        }

        @Override
        public ConfigDef configDef() {
            return null;
        }

        public RecordFilterPipeline<FileInputRecord> onFailure() {
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