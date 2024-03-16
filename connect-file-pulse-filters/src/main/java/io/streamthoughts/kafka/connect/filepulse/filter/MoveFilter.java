/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.MoveFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Moves an existing object value to a given target path.
 */
public class MoveFilter extends AbstractRecordFilter<MoveFilter> {

    private MoveFilterConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new MoveFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return MoveFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        final TypedValue removed = record.remove(config.source());
        if (removed != null) {
            record.insert(config.target(), removed);
            return new RecordsIterable<>(record);

        } else if (!config.ignoreMissing()) {
            throw new FilterException(
                "Cannot move '" + config.source() + "' to '" + config.target() + "' due to field does not exist.");
        }

        return new RecordsIterable<>(record);
    }
}
