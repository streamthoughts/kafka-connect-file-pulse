/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

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
import java.util.Collection;
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
        return GrokFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                 final TypedStruct record) throws FilterException {

        final TypedValue value = record.find(config.source());
        
        if (value == null) {
            throw new FilterException("Invalid field '" + config.source() + "', field does not exist");
        }

        final List<String> valuesToProcess = resolveValues(value);
        final List<TypedStruct> extractedResults = new ArrayList<>();

        for (String valueToProcess : valuesToProcess) {
            extractedResults.add(applyFilterOnValue(valueToProcess));
        }

        return buildResult(extractedResults);
    }

    /**
     * Normalizes the configured source field into the list of strings to run through the Grok patterns.
     *
     * @param value the typed value obtained from the record for the configured source path.
     * @return list of string entries to parse.
     */
    private List<String> resolveValues(final TypedValue value) {
        if (value.type() == Type.STRING) {
            return List.of(value.getString());
        }

        if (value.type() == Type.ARRAY) {
            final Collection<Object> array = value.getArray();
            final List<String> values = new ArrayList<>(array.size());
            for (Object item : array) {
                if (!(item instanceof String)) {
                    throw new FilterException(
                            "Array contains non-string element of type: " + item.getClass().getName());
                }
                values.add((String) item);
            }
            return values;
        }

        throw new FilterException("Source field must be either STRING or ARRAY type, got: " + value.type());
    }

    /**
     * Applies all configured Grok patterns on the given value and returns the resulting struct.
     *
     * @param value the string payload to match.
     * @return the struct built from captured groups.
     */
    private TypedStruct applyFilterOnValue(final String value) {
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
        return mergeToStruct(allNamedCaptured, schema);
    }

    /**
     * Builds the output payload by honoring the target field and the number of extracted results.
     *
     * @param results list of extracted structs for each processed value.
     * @return iterable wrapping the final record to merge.
     */
    private RecordsIterable<TypedStruct> buildResult(final List<TypedStruct> results) {
        final boolean hasTarget = config.target() != null;
        final String targetField = hasTarget ? config.target() : config.source();

        if (results.size() == 1 && !hasTarget) {
            return RecordsIterable.of(results.get(0));
        }

        final TypedStruct result = TypedStruct.create();
        if (results.size() == 1) {
            result.insert(targetField, results.get(0));
        } else {
            result.insert(targetField, TypedValue.array(results, Type.STRUCT));
        }
        return RecordsIterable.of(result);
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