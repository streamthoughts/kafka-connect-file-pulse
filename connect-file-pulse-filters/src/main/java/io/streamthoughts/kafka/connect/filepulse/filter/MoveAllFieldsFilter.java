/*
 * Copyright 2023 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import static java.util.Objects.requireNonNull;

import io.streamthoughts.kafka.connect.filepulse.config.MoveAllFieldsFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigDef;


public class MoveAllFieldsFilter extends AbstractRecordFilter<MoveAllFieldsFilter> {
    private MoveAllFieldsFilterConfig config;

    @Override
    public ConfigDef configDef() {
        return MoveAllFieldsFilterConfig.configDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        this.config = new MoveAllFieldsFilterConfig(configs);
    }

    @Override
    public RecordsIterable<TypedStruct> apply(FilterContext context,
                                              TypedStruct record,
                                              boolean hasNext) throws FilterException {
        return Optional.ofNullable(record)
                .map(this::addTarget)
                .stream()
                .peek(r -> r.schema().fields()
                        .stream()
                        .filter(typedField -> !typedField.name().equals(this.config.target()))
                        .filter(typedField -> !this.config.excludes().contains(typedField.name()))
                        .forEach(typedField -> Optional.ofNullable(requireNonNull(record).remove(typedField.name()))
                                .ifPresent(removed -> {
                                    TypedStruct target = record.getStruct(this.config.target());
                                    target.insert(typedField.name(), removed);
                                })))
                .findFirst()
                .map(RecordsIterable::of)
                .orElse(RecordsIterable.empty());
    }

    private TypedStruct addTarget(TypedStruct record) {
        if (!record.has(this.config.target())) {
            record.put(this.config.target(), TypedStruct.create());
        }

        return record;
    }
}
