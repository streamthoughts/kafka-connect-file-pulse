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

import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputRecord;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public interface RecordFilter extends Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    void configure(final Map<String, ?> configs);

    /**
     * Configuration specification for this filter.
     * @return a new {@link ConfigDef} instance.
     */
    ConfigDef configDef();

    /**
     * Returns the string label to uniquely identify a data filter (useful for debugging).
     *
     * @return a string label.
     */
    default String label() {
        return getClass().getSimpleName();
    }

    /**
     * Filters the specified records.
     *
     * @param context   the filter execution context.
     * @param record    the data to apply.
     * @param hasNext   is there is still incoming records.
     *
     * @throws FilterException if an occurred while filtering input record.
     * @return the output filtered records.
     */
    RecordsIterable<FileInputData> apply(final FilterContext context,
                                         final FileInputData record,
                                         final boolean hasNext) throws FilterException;

    /**
     * Clears all internal states (i.s buffered records)
     * This method is invoke each time records from a new file is starting to be filtered.
     */
    default void clear() {

    }

    /**
     * Flushes any remaining buffered input records.
     *
     * @return an iterable of {@link FileInputRecord} to be flushed.
     */
    default RecordsIterable<FileInputRecord> flush() {
        return RecordsIterable.empty();
    }

    /**
     * Checks whether this filter should be apply on the input {@link FileInputRecord}.
     * @return {@code true} if the filter must be applied.
     */
    default boolean accept(final FilterContext context, final FileInputData record) {
        return true;
    }

    /**
     * Returns the {@link RecordFilterPipeline} to apply when an error occurred while executing this {@link RecordFilter}.
     * If {@code null} is returned then {@link #ignoreFailure()} is invoked to determine if the filter must be skip
     * or the pipeline executing must be halt immediately.
     *
     * @return either a new {@link RecordFilterPipeline} instance or {@code null}.
     */
    default RecordFilterPipeline<FileInputRecord> onFailure() {
        return null;
    }

    /**
     * Skips this filter on failure and continue to next one.
     *
     * @return {@code true} if the failure must be ignored.
     */
    default boolean ignoreFailure() {
        return false;
    }
}
