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

import io.streamthoughts.kafka.connect.filepulse.config.JSONFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.json.DefaultJSONStructConverter;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

public class JSONFilter extends AbstractMergeRecordFilter<JSONFilter> {

    private final DefaultJSONStructConverter converter = new DefaultJSONStructConverter();

    private JSONFilterConfig configs;

    private String source;

    private String target;

    private Charset charset;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new JSONFilterConfig(props);
        source = configs.source();
        target = configs.target();
        charset = configs.charset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return JSONFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<TypedStruct> apply(final FilterContext context, final TypedStruct record) {
        final String value = extractJsonField(checkIsNotNull(record.get(source)));
        try {
            final TypedStruct json = converter.readJson(value);
            if (target != null) {
                record.put(target, json);
                return RecordsIterable.of(record);
            }
            return RecordsIterable.of(json);
        } catch (Exception e) {
            throw new FilterException(e.getLocalizedMessage(), e.getCause());
        }
    }

    private String extractJsonField(final TypedValue value) {
        switch (value.type()) {
            case STRING:
                return value.getString();
            case BYTES:
                return new String(value.getBytes(), charset);
            default:
                throw new FilterException(
                        "Invalid field '" + source + "', cannot parse JSON field of type '" + value.type() + "'");
        }
    }

    private TypedValue checkIsNotNull(final TypedValue value) {
        if (value.isNull()) {
            throw new FilterException(
                "Invalid field '" + source + "', cannot convert empty value to JSON");
        }
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        return configs.overwrite();
    }
}