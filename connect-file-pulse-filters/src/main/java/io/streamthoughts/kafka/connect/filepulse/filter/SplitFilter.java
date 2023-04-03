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

import io.streamthoughts.kafka.connect.filepulse.config.SplitFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.LinkedList;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Split one or multiples message field's value to array.
 */
public class SplitFilter extends AbstractRecordFilter<SplitFilter> {

    private SplitFilterConfig configs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new SplitFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return SplitFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        for (final String key : configs.split()) {

            if (!record.exists(key)) {
                throw new FilterException("Invalid field name '" + key + "'");
            }

            TypedValue value = record.find(key);
            if (value.type() != Type.STRING) {
                throw new FilterException("Cannot split field '" + key + "' of type '" + value.type() + "'");
            }

            if (value.isNull()) {
                record.put(key, new LinkedList<>());
            } else {
                final String[] values = value.getString().split(configs.splitSeparator());
                record.put(key, values);
            }
        }
        return new RecordsIterable<>(record);
    }
}
