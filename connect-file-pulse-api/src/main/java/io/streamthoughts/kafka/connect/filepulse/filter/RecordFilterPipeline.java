/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.util.List;

/**
 * Default interface to apply all filters on input records.
 *
 * @param <T> type of the input value.
 */
public interface RecordFilterPipeline<T extends FileRecord<TypedStruct>> {

    /**
     * Initialize the value-filter chain for the specified context.
     * @param context   the input file context.
     */
    void init(final FileObjectContext context);

    /**
     * Execute filters on the given records.
     *
     * @param records   the records to be filtered.
     * @param hasNext   flag to indicate if there is remaining records for the current input file.
     *                  This flag should be used by filters to flush buffered records  when equals {@code false}.
     * @return          the filtered records.
     */
    RecordsIterable<T> apply(final RecordsIterable<T> records, final boolean hasNext) throws FilterException;

    /**
     * Execute the filter chain on a single record.
     *
     * @param context   the record context.
     * @param record    the record to be filtered.
     * @param hasNext   flag to indicate if there is remaining records for the current input file.
     *                  This flag should be used by filters to flush buffered records  when equals {@code false}.
     * @return          the filtered records.
     */
    List<T> apply(final FilterContext context,
                  final TypedStruct record,
                  final boolean hasNext);
}
