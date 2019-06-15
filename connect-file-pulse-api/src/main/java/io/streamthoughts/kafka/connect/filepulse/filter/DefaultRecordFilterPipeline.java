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
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.stream.Collectors;

public class DefaultRecordFilterPipeline implements RecordFilterPipeline<FileInputRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRecordFilterPipeline.class);

    private final FilterNode node;

    private FileInputContext context;

    /**
     * Creates a new {@link RecordFilterPipeline} instance.
     * @param filters   the list of filters.
     */
    public DefaultRecordFilterPipeline(final List<RecordFilter> filters) {
        Objects.requireNonNull(filters, "filters can't be null");

        ListIterator<RecordFilter> filterIterator = filters.listIterator(filters.size());
        FilterNode next = null;
        while (filterIterator.hasPrevious()) {
            next = new FilterNode(filterIterator.previous(), next);
        }
        node = next;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final FileInputContext context) {
        this.context = context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileInputRecord> apply(final RecordsIterable<FileInputRecord> records,
                                                  final boolean hasNext) {
        checkState();
        List<FileInputRecord> results = new LinkedList<>();
        final Iterator<FileInputRecord> iterator = records.iterator();
        while (iterator.hasNext()) {
            FileInputRecord record = iterator.next();
            boolean doHasNext = hasNext || iterator.hasNext();
            InternalFilterContext context =
                InternalFilterContext.with(this.context.metadata(), record.offset(), record.data());
            results.addAll( apply(context, record.data(), doHasNext));
        }
        return new RecordsIterable<>(results);
    }

    private void checkState() {
        if (context == null) {
            throw new IllegalStateException("Cannot apply this pipeline, no context initialized");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FileInputRecord> apply(final FilterContext context,
                                       final FileInputData record,
                                       final boolean hasNext) {
        return node.apply(context, record, hasNext);
    }

    private class FilterNode {

        private final RecordFilter current;
        private final FilterNode onSuccess;

        /**
         * Creates a new {@link FilterNode} instance.
         *
         * @param current
         * @param onSuccess
         */
        private FilterNode(final RecordFilter current,
                           final FilterNode onSuccess) {
            this.current = current;
            this.onSuccess = onSuccess;
        }

        public List<FileInputRecord> apply(final FilterContext context,
                                           final FileInputData record,
                                           final boolean hasNext) {

            final List<FileInputRecord> filtered = new LinkedList<>();

            if (current.accept(context, record)) {
                try {
                    RecordsIterable<FileInputData> data = current.apply(context, record, hasNext);
                    List<FileInputRecord> records = data
                        .stream()
                        .map(s -> new FileInputRecord(context.offset(), s))
                        .collect(Collectors.toList());
                    filtered.addAll(records);
                } catch (final Exception e) {
                    RecordFilterPipeline<FileInputRecord> pipelineOnFailure = current.onFailure();

                    if (pipelineOnFailure != null || current.ignoreFailure()) {
                        // Some filter could buffered records to build aggregates,
                        // those buffered records are normally return at certain time while the method apply is invoked.
                        //
                        // When current filter is not accepting current data and
                        // no records are remaining we should force a flush.
                        RecordsIterable<FileInputRecord> buffered = current.flush();

                        if (onSuccess != null) {
                            filtered.addAll(buffered.stream()
                                .flatMap(r -> {
                                    // create a new context for buffered records.
                                    InternalFilterContext localContext =
                                        InternalFilterContext.with(context.metadata(), r.offset(), r.data());
                                    return onSuccess.apply(localContext, r.data(), hasNext).stream();
                                })
                                .collect(Collectors.toList()));
                        } else {
                            filtered.addAll(buffered.collect());
                        }
                    }

                    if (pipelineOnFailure != null) {
                        final InternalFilterContext errorContext = InternalFilterContext.with(
                            context.metadata(),
                            context.offset(),
                            record,
                            context.values(),
                            new ExceptionContext(e.getLocalizedMessage(), current.label()));
                        filtered.addAll(pipelineOnFailure.apply(errorContext, record, hasNext));
                    } else if (current.ignoreFailure()) {
                        if (onSuccess != null) {
                            filtered.addAll(onSuccess.apply(context, record, hasNext));
                        } else {
                            filtered.add(new FileInputRecord(context.offset(), record));
                        }
                    } else {
                        LOG.error(
                            "Error occurred while executing filter '{}' with schema='{}', record='{}'",
                            current.label(),
                            record.schema(),
                            record.value());
                        throw e;
                    }
                    return filtered;
                }
                // If no exception occurred previously then forward to the next filter.
                if (onSuccess != null) {
                    return filtered
                        .stream()
                        .flatMap(r -> {
                            final InternalFilterContext localContext = InternalFilterContext.with(
                                context.metadata(),
                                context.offset(),
                                r.data(),
                                context.values(),
                                context.exception());
                            return onSuccess.apply(localContext, r.data(), hasNext).stream();
                        })
                        .collect(Collectors.toList());
                }

                return filtered;

            } else if (!hasNext) {
                // Some filter could buffered records to build aggregates,
                // those buffered records are normally return at certain time while the method apply is invoked.
                //
                // When current filter is not accepting current data and
                // no records are remaining we should force a flush.
                RecordsIterable<FileInputRecord> buffered = current.flush();
                if (onSuccess!= null) {
                    filtered.addAll(buffered.stream()
                        .flatMap(r -> {
                            // create a new context for buffered records.
                            InternalFilterContext localContext =
                                InternalFilterContext.with(context.metadata(), r.offset(), r.data());
                            return onSuccess.apply(localContext, r.data(), hasNext).stream();
                        })
                        .collect(Collectors.toList()));
                } else {
                    filtered.addAll(buffered.collect());
                }
            }

            if (onSuccess != null) {
                filtered.addAll(onSuccess.apply(context, record, hasNext));
            } else {
                filtered.add(new FileInputRecord(context.offset(), record));
            }

           return filtered;
        }
    }
}
