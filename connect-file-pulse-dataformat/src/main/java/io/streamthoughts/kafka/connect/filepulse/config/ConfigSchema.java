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
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class ConfigSchema implements Supplier<Schema> {

    public static ConfigSchema fromConnectSchema(Schema schema) {
        Schema.Type type = schema.type();

        ConfigSchema configSchema = new ConfigSchema();
        configSchema.type = type;
        configSchema.name = schema.name();
        configSchema.doc = schema.doc();
        configSchema.defaultValue = schema.defaultValue();
        configSchema.parameters = schema.parameters();
        configSchema.isOptional = schema.isOptional();
        switch (type) {
            case MAP:
                configSchema.keySchema = fromConnectSchema(schema.keySchema());
                configSchema.valueSchema = fromConnectSchema(schema.valueSchema());
                break;
            case ARRAY:
                configSchema.valueSchema = fromConnectSchema(schema.valueSchema());
                break;
            case STRUCT:
                configSchema.fieldSchemas = new HashMap<>();
                schema.fields().stream()
                    .sorted(Comparator.comparing(Field::index))
                    .forEachOrdered(f -> configSchema.fieldSchemas.put(f.name(), fromConnectSchema(f.schema())));
                break;
            default:
                break;
        }
        return configSchema;
    }

    public Schema.Type type;
    public boolean isOptional;

    public String name;

    public Integer version;
    public Object defaultValue;
    public String doc;
    public Map<String, String> parameters;
    public ConfigSchema keySchema;
    public ConfigSchema valueSchema;
    public Map<String, ConfigSchema> fieldSchemas;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Schema get() {
        final SchemaBuilder builder;
        switch (this.type) {
            case MAP:
                Objects.requireNonNull(keySchema, "keySchema cannot be null.");
                Objects.requireNonNull(valueSchema, "valueSchema cannot be null.");
                builder = SchemaBuilder.map(keySchema.get(), valueSchema.get());
                break;
            case ARRAY:
                Objects.requireNonNull(valueSchema, "valueSchema cannot be null.");
                builder = SchemaBuilder.array(valueSchema.get());
                break;
            default:
                builder = SchemaBuilder.type(type);
                break;
        }

        if (Schema.Type.STRUCT == type) {
            fieldSchemas.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEachOrdered(it -> builder.field(it.getKey(), it.getValue().get()));
        }

        if (StringUtils.isNotBlank(name))
            builder.name(name);

        if (StringUtils.isNotBlank(doc))
            builder.doc(doc);

        if (null != defaultValue) {
            Object value;
            switch (type) {
                case INT8:
                    value = ((Number) defaultValue).byteValue();
                    break;
                case INT16:
                    value = ((Number) defaultValue).shortValue();
                    break;
                case INT32:
                    value = ((Number) defaultValue).intValue();
                    break;
                case INT64:
                    value = ((Number) defaultValue).longValue();
                    break;
                case FLOAT32:
                    value = ((Number) defaultValue).floatValue();
                    break;
                case FLOAT64:
                    value = ((Number) defaultValue).doubleValue();
                    break;
                default:
                    value = defaultValue;
                    break;
            }
            builder.defaultValue(value);
        }

        if (null != parameters) {
            builder.parameters(parameters);
        }

        if (isOptional) {
            builder.optional();
        }

        if (null != version) {
            builder.version(version);
        }
        return builder.build();
    }
}
