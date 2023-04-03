/*
 * Copyright 2019-2020 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.config.CommonFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.filter.condition.FilterCondition;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;

public abstract class AbstractRecordFilter<T extends AbstractRecordFilter<T>> implements RecordFilter {

    private RecordFilterPipeline<FileRecord<TypedStruct>> failurePipeline;

    FilterCondition condition;

    private boolean ignoreFailure;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract ConfigDef configDef();

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        // intentionally left blank
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props, final RecordFilterProvider provider) {
        CommonFilterConfig filterConfig = new CommonFilterConfig(props);
        withOnCondition(filterConfig.condition());
        withIgnoreFailure(filterConfig.ignoreFailure());
        if (!filterConfig.onFailure().isEmpty()) {
            withOnFailure(
                new DefaultRecordFilterPipeline(filterConfig.onFailure().stream()
                    .map(provider::getRecordForAlias)
                    .collect(Collectors.toList())
                )
            );
        }
        configure(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                       final TypedStruct record,
                                                       final boolean hasNext) throws FilterException;

    @SuppressWarnings("unchecked")
    public T withOnCondition(final FilterCondition condition) {
        this.condition = condition;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withOnFailure(final RecordFilterPipeline<FileRecord<TypedStruct>> failurePipeline) {
        this.failurePipeline = failurePipeline;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withIgnoreFailure(final boolean ignoreFailure) {
        this.ignoreFailure = ignoreFailure;
        return (T) this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final FilterContext context, final TypedStruct record) {
        return condition.apply(context, record);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordFilterPipeline<FileRecord<TypedStruct>> onFailure() {
        return failurePipeline;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean ignoreFailure() {
        return ignoreFailure;
    }
}
