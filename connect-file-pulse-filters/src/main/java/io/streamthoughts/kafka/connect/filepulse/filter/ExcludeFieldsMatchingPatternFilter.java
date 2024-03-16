/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.config.ExcludeFieldsMatchingPatternConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import org.apache.kafka.common.config.ConfigDef;

public class ExcludeFieldsMatchingPatternFilter extends AbstractRecordFilter<ExcludeFieldsMatchingPatternFilter> {

    private ExcludeFieldsMatchingPatternConfig config;

    @Override
    public ConfigDef configDef() {
        return ExcludeFieldsMatchingPatternConfig.configDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        this.config = new ExcludeFieldsMatchingPatternConfig(configs);
    }

    @Override
    public RecordsIterable<TypedStruct> apply(FilterContext filterContext,
                                              TypedStruct record,
                                              boolean hasNext) throws FilterException {

        return Optional.ofNullable(record)
                .stream()
                .peek(r -> r.schema().fields()
                        .stream()
                        .forEach(typedField -> {
                            Optional.ofNullable(r.get(typedField.name()).getString())
                                    .ifPresentOrElse(fieldValue -> {
                                        Matcher matcher = this.config.pattern().matcher(fieldValue);
                                        if (matcher.matches() && !config.blockField()) {
                                            r.put(typedField.name(), TypedValue.string(null));
                                        } else if (matcher.matches()) {
                                            r.remove(typedField.name());
                                        }
                                    }, () -> {
                                        if (!config.blockField()) {
                                            r.put(typedField.name(), TypedValue.string(null));
                                        } else {
                                            r.remove(typedField.name());
                                        }
                                    });


                        }))
                .findFirst()
                .map(RecordsIterable::of)
                .orElse(RecordsIterable.empty());
    }
}
