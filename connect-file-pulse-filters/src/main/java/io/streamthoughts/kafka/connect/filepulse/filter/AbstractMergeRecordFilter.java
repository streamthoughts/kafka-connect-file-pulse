/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.merger.DefaultTypeValueMerger;
import io.streamthoughts.kafka.connect.filepulse.data.merger.TypeValueMerger;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMergeRecordFilter<T extends AbstractRecordFilter<T>> extends AbstractRecordFilter<T> {

    private final TypeValueMerger merger = new DefaultTypeValueMerger();

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        final RecordsIterable<TypedStruct> filtered = apply(context, record);

        final List<TypedStruct> merged = filtered
                .stream()
                .map(it -> Optional.ofNullable(it)
                    // only non-null records should be merged
                    .map(o -> merger.merge(record, o, overwrite()))
                    .orElse(null)
                )
                .collect(Collectors.toList());

        return new RecordsIterable<>(merged);
    }

    /**
     * Apply the filter logic for the specified value.
     *
     * @param context the {@link FilterContext} instance.
     * @param record  the {@link TypedStruct} instance to filter.
     *
     * @return  a new {@link RecordsIterable} instance.
     */
    abstract protected RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                          final TypedStruct record) throws FilterException ;

    /**
     * Returns a list of fields that must be overwrite.
     *
     * @return a set of field names.
     */
    abstract protected Set<String> overwrite();
}
