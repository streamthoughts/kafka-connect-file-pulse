/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter.condition;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.filter.FilterContext;
import io.streamthoughts.kafka.connect.filepulse.filter.FilterException;

/**
 * Default interface to determine if a {@link io.streamthoughts.kafka.connect.filepulse.filter.RecordFilter}
 * must be applied.
 */
public interface FilterCondition {

    FilterCondition TRUE = (ctx, data) -> true;

    /**
     * Checks whether a filter can be applied on the specified {@link TypedStruct}.
     *
     * @param context   the filter execution context.
     * @param record    the value to apply.
     *
     * @return {@code true} if filter can be applied.
     */
    boolean apply(final FilterContext context, final TypedStruct record) throws FilterException;


    static FilterCondition revert(final FilterCondition condition) {
        return (context, record) -> ! condition.apply(context, record);
    }
}
