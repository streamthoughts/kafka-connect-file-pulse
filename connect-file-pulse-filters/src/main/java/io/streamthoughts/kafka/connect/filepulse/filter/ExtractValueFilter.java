/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.ExtractValueConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import org.apache.kafka.common.config.ConfigDef;

public class ExtractValueFilter extends AbstractRecordFilter<ExtractValueFilter> {

    private ExtractValueConfig config;

    @Override
    public ConfigDef configDef() {
        return ExtractValueConfig.configDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        this.config = new ExtractValueConfig(configs);
    }

    @Override
    public RecordsIterable<TypedStruct> apply(FilterContext filterContext,
                                              TypedStruct record,
                                              boolean hasNext) throws FilterException {

        String targetField = Optional.ofNullable(config.getTargetName()).orElse(config.getFieldName());

        return Optional.ofNullable(record)
                .map(r -> r.get(config.getFieldName()))
                .map(fieldValue -> {

                    Matcher matcher = config.pattern().matcher(fieldValue.getString());

                    if (matcher.matches() && matcher.groupCount() > 0) {
                        record.put(targetField, TypedValue.string(matcher.group(1)));
                    } else {
                        record.put(targetField, TypedValue.string(config.getDefaultValue()));
                    }
                    return record;
                })
                .map(RecordsIterable::of)
                .orElse(RecordsIterable.empty());
    }
}
