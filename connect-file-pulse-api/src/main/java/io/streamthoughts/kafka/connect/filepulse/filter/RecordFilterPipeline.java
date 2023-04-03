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
