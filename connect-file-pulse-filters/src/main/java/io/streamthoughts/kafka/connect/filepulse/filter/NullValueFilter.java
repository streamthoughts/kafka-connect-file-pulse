/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.common.config.ConfigDef;

/**
 * A {@link RecordFilter} which can be used to empty a record-value to null.
 */
public class NullValueFilter extends AbstractRecordFilter<NullValueFilter> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return CommonFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        return condition.apply(context, record) ? RecordsIterable.of((TypedStruct)null) : RecordsIterable.of(record);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final FilterContext context, final TypedStruct record) {
        // We should always accept record to filter into the apply method.
        return true;
    }
}
