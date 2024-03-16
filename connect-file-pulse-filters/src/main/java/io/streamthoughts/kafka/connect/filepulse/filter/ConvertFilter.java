/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import static io.streamthoughts.kafka.connect.filepulse.config.ConvertFilterConfig.CONVERT_IGNORE_MISSING_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.config.ConvertFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class ConvertFilter extends AbstractRecordFilter<ConvertFilter> {

    private ConvertFilterConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new ConvertFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return ConvertFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        final String fieldName = config.field();
        final TypedValue value = record.find(fieldName);
        if (value != null) {
            try {
                TypedValue converted = value.as(config.to());
                record.insert(fieldName, converted);
            } catch (Exception e) {
                if (config.defaultValue() == null) {
                    throw new FilterException(
                        "Fail to convert field '" + fieldName + "' to type " + config.to() + ": " + e.getMessage()
                    );
                }
                record.insert(fieldName, config.defaultValue());
            }

        } else if (!config.ignoreMissing()) {
            throw new FilterException(
                "Cannot find field for name '" + fieldName + "' (" + CONVERT_IGNORE_MISSING_CONFIG+ "=false)"
            );
        }

        return RecordsIterable.of(record);
    }
}
