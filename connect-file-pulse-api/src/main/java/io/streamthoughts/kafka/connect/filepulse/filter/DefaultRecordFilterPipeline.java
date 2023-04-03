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

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import io.streamthoughts.kafka.connect.filepulse.source.TypedFileRecord;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRecordFilterPipeline implements RecordFilterPipeline<FileRecord<TypedStruct>> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRecordFilterPipeline.class);

    private final FilterNode rootNode;

    private FileObjectContext fileObjectObject;

    /**
     * Creates a new {@link RecordFilterPipeline} instance.
     *
     * @param filters the list of filters.
     */
    public DefaultRecordFilterPipeline(final List<RecordFilter> filters) {
        Objects.requireNonNull(filters, "filters can't be null");

        ListIterator<RecordFilter> filterIterator = filters.listIterator(filters.size());
        FilterNode next = null;
        while (filterIterator.hasPrevious()) {
            next = new FilterNode(filterIterator.previous(), next);
        }
        rootNode = next;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final FileObjectContext fileObjectObject) {
        this.fileObjectObject = fileObjectObject;
        FilterNode node = rootNode;
        while (node != null) {
            // Initialize on failure pipeline
            RecordFilterPipeline<FileRecord<TypedStruct>> pipelineOnFailure = node.filter.onFailure();
            if (pipelineOnFailure != null) {
                pipelineOnFailure.init(fileObjectObject);
            }
            // Prepare filter for next input file.
            node.filter.clear();
            node = node.onSuccess;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileRecord<TypedStruct>> apply(final RecordsIterable<FileRecord<TypedStruct>> records,
                                                          final boolean hasNext) throws FilterException {
        checkState();

        if (rootNode == null) {
            return records;
        }

        List<FileRecord<TypedStruct>> results = new LinkedList<>();
        final Iterator<FileRecord<TypedStruct>> iterator = records.iterator();
        while (iterator.hasNext()) {
            FileRecord<TypedStruct> record = iterator.next();
            boolean doHasNext = hasNext || iterator.hasNext();
            // Create new context for current record and file-object metadata.
            FilterContext context = newContextFor(record.offset(), fileObjectObject.metadata());
            // Apply the filter-chain on current record.
            results.addAll(apply(context, record.value(), doHasNext));
        }
        return new RecordsIterable<>(results);
    }

    private FilterContext newContextFor(final FileRecordOffset offset,
                                        final FileObjectMeta metadata) {
        return FilterContextBuilder
                .newBuilder()
                .withMetadata(metadata)
                .withOffset(offset)
                .build();
    }

    private void checkState() {
        if (fileObjectObject == null) {
            throw new IllegalStateException("Cannot apply this pipeline, no context initialized");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FileRecord<TypedStruct>> apply(final FilterContext context,
                                               final TypedStruct record,
                                               final boolean hasNext) {
        if (rootNode == null) {
            return Collections.singletonList(
                    new TypedFileRecord(context.offset(), record)
            );
        }
        return rootNode.apply(context, record, hasNext);
    }

    private class FilterNode {

        private final RecordFilter filter;
        private final FilterNode onSuccess;

        /**
         * Creates a new {@link FilterNode} instance.
         *
         * @param filter    the current filter.
         * @param onSuccess the next filter ot be applied on success.
         */
        private FilterNode(final RecordFilter filter,
                           final FilterNode onSuccess) {
            this.filter = filter;
            this.onSuccess = onSuccess;
        }

        public List<FileRecord<TypedStruct>> apply(final FilterContext context,
                                                   final TypedStruct record,
                                                   final boolean hasNext) {

            final List<FileRecord<TypedStruct>> filtered = new LinkedList<>();

            // Check whether current filter does NOT accept this record.
            if (!filter.accept(context, record)) {
                // skip current filter and forward record to the next one.
                if (onSuccess != null) {
                    filtered.addAll(onSuccess.apply(context, record, hasNext));
                } else {
                    if (!hasNext) {
                        filtered.addAll(flush(context));
                    }
                    // add current record to filtered result and make sure to copy all context information.
                    filtered.add(newRecordFor(context, record));
                }
                return filtered;
            }

            final RecordsIterable<TypedStruct> data;
            try {
                data = filter.apply(context, record, hasNext);
            } catch (final Exception e) {
                RecordFilterPipeline<FileRecord<TypedStruct>> onFailure = filter.onFailure();

                if (onFailure == null && !filter.ignoreFailure()) {
                    LOG.error(
                            "Failed to execute filter '{}' on record at offset '{}' from object-file {}. Error: {}",
                            filter.label(),
                            context.offset(),
                            context.metadata(),
                            e.getLocalizedMessage()
                    );
                    throw e;
                } else {
                    LOG.debug("Failed to execute filter '{}' on record at offset '{}' from object-file {}. Error: {} "
                           +  "Exception will be either ignored or handled by a dedicated filter chain.",
                            filter.label(),
                            context.offset(),
                            context.metadata(),
                            e.getLocalizedMessage()
                    );
                }

                // Some filters can aggregate records which follow each other by maintaining internal buffers.
                // Those buffered records are expected to be returned at a certain point in time on the
                // invocation of the method apply.
                // When an error occurred, current record can be ignored or forward to an error pipeline.
                // Thus, following records can potentially trigger unexpected aggregates to be built.
                // To address that we force a flush of all records still buffered by the current filter.
                List<FileRecord<TypedStruct>> flushed = flush(context);
                filtered.addAll(flushed);

                if (onFailure != null) {
                    final FilterContext errorContext = FilterContextBuilder
                            .newBuilder(context)
                            .withError(FilterError.of(e, filter.label()))
                            .build();
                    filtered.addAll(onFailure.apply(errorContext, record, hasNext));
                    // Ignore exception (i.e. ignoreFailure = true)
                } else {
                    if (onSuccess != null) {
                        filtered.addAll(onSuccess.apply(context, record, hasNext));
                    } else {
                        filtered.add(newRecordFor(context, record));
                    }
                }
                return filtered;
            }

            List<FileRecord<TypedStruct>> records = data
                    .stream()
                    .map(s -> newRecordFor(context, s))
                    .collect(Collectors.toList());
            filtered.addAll(records);

            if (onSuccess == null) {
                return filtered;
            }
            return filtered
                    .stream()
                    .flatMap(r ->
                            onSuccess.apply(FilterContextBuilder.newBuilder(context).build(), r.value(), hasNext)
                                    .stream()
                    )
                    .collect(Collectors.toList());

        }

        private TypedFileRecord newRecordFor(final FilterContext context, final TypedStruct object) {
            return new TypedFileRecord(context.offset(), object)
                    .withTopic(context.topic())
                    .withPartition(context.partition())
                    .withTimestamp(context.timestamp())
                    .withHeaders(context.headers())
                    .withKey(TypedValue.string(context.key()));
        }

        /**
         * Flush and apply the filter chain on any remaining records buffered by this.
         *
         * @param context the filter context to be used.
         */
        List<FileRecord<TypedStruct>> flush(final FilterContext context) {

            final List<FileRecord<TypedStruct>> filtered = new LinkedList<>();

            RecordsIterable<FileRecord<TypedStruct>> buffered = filter.flush();

            if (onSuccess != null) {
                Iterator<FileRecord<TypedStruct>> iterator = buffered.iterator();
                while (iterator.hasNext()) {
                    final FileRecord<TypedStruct> record = iterator.next();
                    // create a new context for buffered records.
                    final FilterContext renewedContext = newContextFor(record.offset(), context.metadata());
                    filtered.addAll(onSuccess.apply(renewedContext, record.value(), iterator.hasNext()));
                }
            } else {
                filtered.addAll(buffered.collect());
            }
            return filtered;
        }
    }
}