/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.RenameFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class RenameFilter extends AbstractRecordFilter<RenameFilter> {

    private RenameFilterConfig configs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new RenameFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return RenameFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        if (record.exists(configs.field())) {
            record.rename(configs.field(), configs.target());
            return new RecordsIterable<>(record);

        } else if (!configs.ignoreMissing()) {
            throw new FilterException("Invalid field name '" + configs.field() + "'");
        }

        return new RecordsIterable<>(record);
    }
}
