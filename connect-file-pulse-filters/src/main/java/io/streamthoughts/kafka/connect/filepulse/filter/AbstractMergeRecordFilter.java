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
