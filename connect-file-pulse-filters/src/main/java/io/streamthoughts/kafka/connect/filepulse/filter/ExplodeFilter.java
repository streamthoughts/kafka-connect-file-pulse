/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.ExplodeFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;

/**
 * The {@link ExplodeFilter} explodes an array or list field into separate records.
 */
public class ExplodeFilter extends AbstractMergeRecordFilter<ExplodeFilter>  {

    private ExplodeFilterConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new ExplodeFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return ExplodeFilterConfig.configDef();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                 final TypedStruct record) throws FilterException {
        final TypedValue value = checkIsNotNull(record.find(config.source()));

        if (value.type() != Type.ARRAY) {
            throw new FilterException(
                "Invalid type for field '" + config.source()  + "', expected ARRAY, was " + value.type());
        }

        final List<TypedStruct> explode = value.getArray()
                .stream()
                .map(it -> TypedStruct.create().insert(config.source(), TypedValue.any(it)))
                .collect(Collectors.toList());

        return new RecordsIterable<>(explode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        Set<String> overwrite = new HashSet<>();
        overwrite.add(config.source());
        return overwrite;
    }

    private TypedValue checkIsNotNull(final TypedValue value) {
        if (value.isNull()) {
            throw new FilterException("Invalid field '" + config.source() + "', cannot explode empty value");
        }
        return value;
    }
}
