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
import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.json.JSONStructConverter;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;

public class JSONFilter extends AbstractMergeRecordFilter<JSONFilter> {

    private final JSONStructConverter converter = JSONStructConverter.createDefault();

    private JSONFilterConfig configs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new JSONFilterConfig(props);
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
        final String value = extractJsonField(checkIsNotNull(record.get(configs.source())));
        final TypedValue typedValue;

        try {
            typedValue = converter.readJson(value);
        } catch (Exception e) {
            throw new FilterException(e.getLocalizedMessage(), e.getCause());
        }

        final Type type = typedValue.type();

        if (type != Type.ARRAY && type != Type.STRUCT) {
            throw new FilterException(
                "Cannot process JSON value with unsupported type. Expected Array or Object, was " + type);
        }

        if (type == Type.STRUCT && configs.merge()) {
            return RecordsIterable.of(typedValue.getStruct());
        }

        if (type == Type.ARRAY) {
            if (configs.explode()) {

                Collection<?> items = typedValue.getArray();
                ArraySchema arraySchema = (ArraySchema)typedValue.schema();
                Type arrayValueType = arraySchema.valueSchema().type();

                if (configs.merge()) {
                    if (arrayValueType == Type.STRUCT) {
                        final List<TypedStruct> records = items
                            .stream()
                            .map(it -> TypedValue.any(it).getStruct())
                            .collect(Collectors.toList());
                        return new RecordsIterable<>(records);
                    }

                    throw new FilterException(
                        "Unsupported operation. Cannot merge array value of type '"
                         + arrayValueType + "' into the top level of the input record");
                }

                final List<TypedStruct> records = items
                    .stream()
                    .map(it -> TypedStruct.create().put(targetField(), TypedValue.of(it, arrayValueType)))
                    .collect(Collectors.toList());
                return new RecordsIterable<>(records);
            }

            if (configs.merge()) {
                throw new FilterException(
                    "Unsupported operation. Cannot merge JSON Array into the top level of the input record");
            }
        }

        return RecordsIterable.of(TypedStruct.create().put(targetField(), typedValue));
    }

    private String extractJsonField(final TypedValue value) {
        switch (value.type()) {
            case STRING:
                return value.getString();
            case BYTES:
                return new String(value.getBytes(), configs.charset());
            default:
                throw new FilterException(
                    "Invalid field '" + configs.source() + "', cannot parse JSON field of type '" + value.type() + "'"
                );
        }
    }

    private TypedValue checkIsNotNull(final TypedValue value) {
        if (value.isNull()) {
            throw new FilterException(
                "Invalid field '" + configs.source() + "', cannot convert empty value to JSON");
        }
        return value;
    }

    private String targetField() {
       return configs.target() != null ? configs.target() : configs.source();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        return configs.target() == null && !configs.merge() ?
                Collections.singleton(configs.source())
                : configs.overwrite() ;
    }
}