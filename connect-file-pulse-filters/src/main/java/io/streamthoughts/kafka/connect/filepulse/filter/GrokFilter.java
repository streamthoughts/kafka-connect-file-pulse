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
import io.streamthoughts.kafka.connect.filepulse.config.GrokFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.transform.pattern.GrokMatcher;
import io.streamthoughts.kafka.connect.transform.pattern.GrokPatternCompiler;
import io.streamthoughts.kafka.connect.transform.pattern.GrokPatternResolver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class GrokFilter extends AbstractMergeRecordFilter<GrokFilter> {

    private GrokFilterConfig config;

    private GrokPatternCompiler compiler;

    private List<GrokMatcher> matchPatterns;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new GrokFilterConfig(props);

        compiler = new GrokPatternCompiler(
                new GrokPatternResolver(config.grok().patternDefinitions(), config.grok().patternsDir()),
                config.grok().namedCapturesOnly());

        matchPatterns = config.grok().patterns()
                .stream()
                .map(pattern -> compiler.compile(pattern))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return CommonFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                 final TypedStruct record) throws FilterException {

        final String value = record.getString(config.source());

        if (value == null) return null;

        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        List<SchemaAndNamedCaptured> allNamedCaptured = new ArrayList<>(matchPatterns.size());

        for (GrokMatcher matcher : matchPatterns) {
            final Map<String, Object> captured = matcher.captures(bytes);
            if (captured != null) {
                allNamedCaptured.add(new SchemaAndNamedCaptured(matcher.schema(), captured));
                if (config.grok().breakOnFirstPattern()) break;
            }
        }

        if (allNamedCaptured.isEmpty()) {
            throw new FilterException("Supplied Grok patterns does not match input data: " + value);
        }

        final Schema schema = mergeToSchema(allNamedCaptured);
        return RecordsIterable.of(mergeToStruct(allNamedCaptured, schema));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        return config.overwrite();
    }

    private Schema mergeToSchema(final List<SchemaAndNamedCaptured> allNamedCaptured) {
        if (allNamedCaptured.size() == 1) return allNamedCaptured.get(0).schema();

        final Map<String, Schema> fields = new HashMap<>();
        for (SchemaAndNamedCaptured namedCaptured : allNamedCaptured) {
            final Schema schema = namedCaptured.schema();
            schema.fields().forEach(f -> {
                final Schema fieldSchema = fields.containsKey(f.name()) ? SchemaBuilder.array(f.schema()) : f.schema();
                fields.put(f.name(), fieldSchema);
            });
        }
        SchemaBuilder schema = SchemaBuilder.struct();
        fields.forEach(schema::field);
        return schema.build();
    }

    private TypedStruct mergeToStruct(final List<SchemaAndNamedCaptured> allNamedCaptured,
                                      final Schema schema) {
        final Map<String, TypedValue> fields = new HashMap<>();
        for (SchemaAndNamedCaptured schemaAndNamedCaptured : allNamedCaptured) {
            schemaAndNamedCaptured.namedCaptured().forEach((name, value) -> {
                final Field field = schema.field(name);
                if (field.schema().type() == Schema.Type.ARRAY) {
                    fields.computeIfAbsent(name, k -> {
                        final Schema valueSchema = field.schema().valueSchema();
                        return TypedValue.array(new ArrayList<>(), Type.forConnectSchemaType(valueSchema.type()));
                    }).getArray().add(value);
                } else
                    fields.put(name, TypedValue.of(value, Type.forConnectSchemaType(field.schema().type())));
            });
        }
        final TypedStruct struct = TypedStruct.create();
        fields.forEach(struct::put);
        return struct;
    }

    private static final class SchemaAndNamedCaptured {

        private final Schema schema;
        private final Map<String, Object> namedCaptured;

        public SchemaAndNamedCaptured(final Schema schema,
                                      final Map<String, Object> namedCaptured) {
            this.schema = schema;
            this.namedCaptured = namedCaptured;
        }

        public Schema schema() {
            return schema;
        }

        public Map<String, Object> namedCaptured() {
            return namedCaptured;
        }
    }

}