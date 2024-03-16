/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;

public interface RecordFilter extends Configurable {

    /**
     * Configure this class with the given key-value pairs
     *
     * @param configs the configuration.
     */
    @Override
    void configure(final Map<String, ?> configs);

    default void configure(final Map<String, ?> configs,
                           final RecordFilterProvider provider) {
        configure(configs);
    }

    /**
     * Configuration specification for this filter.
     *
     * @return a new {@link ConfigDef} instance.
     */
    ConfigDef configDef();

    /**
     * Returns the string label to uniquely identify a value filter (useful for debugging).
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
     * @param record    the value to apply.
     * @param hasNext   is there is still incoming records.
     *
     * @throws FilterException if an occurred while filtering input record.
     * @return the output filtered records.
     */
    RecordsIterable<TypedStruct> apply(final FilterContext context,
                                       final TypedStruct record,
                                       final boolean hasNext) throws FilterException;

    /**
     * Clears all internal states (i.s buffered records)
     * This method is invoked each time records from a new file is starting to be filtered.
     */
    default void clear() {

    }

    /**
     * Flushes any remaining buffered input records.
     *
     * @return an iterable of {@link FileRecord} to be flushed.
     */
    default RecordsIterable<FileRecord<TypedStruct>> flush() {
        return RecordsIterable.empty();
    }

    /**
     * Checks whether this filter should be applied on the input {@link TypedStruct}.
     *
     * @param context   the {@link FilterContext} instance.
     * @param record    the {@link TypedStruct} instance.
     *
     * @return {@code true} if the filter must be applied.
     */
    default boolean accept(final FilterContext context,
                           final TypedStruct record) {
        return true;
    }

    /**
     * Returns the {@link RecordFilterPipeline} to apply when an error occurred while executing this {@link RecordFilter}.
     * If {@code null} is returned then {@link #ignoreFailure()} is invoked to determine if the filter must be skip
     * or the pipeline executing must be halt immediately.
     *
     * @return either a new {@link RecordFilterPipeline} instance or {@code null}.
     */
    default RecordFilterPipeline<FileRecord<TypedStruct>> onFailure() {
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


    @FunctionalInterface
    interface RecordFilterProvider {

        RecordFilter getRecordForAlias(final String alias);
    }
}
