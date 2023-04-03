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
