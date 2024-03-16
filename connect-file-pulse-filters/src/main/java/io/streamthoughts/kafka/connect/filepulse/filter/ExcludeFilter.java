/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.ExcludeFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Excludes one or more fields from the input record.
 */
public class ExcludeFilter extends AbstractRecordFilter<ExcludeFilter> {

    private ExcludeFilterConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return ExcludeFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        this.config = new ExcludeFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {
        config.fields().forEach(record::remove);
        return new RecordsIterable<>(record);
    }
}
