/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_EXTRACT_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_COLUMNS_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.StructSchema;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public abstract class AbstractDelimitedRowFilter<T extends AbstractRecordFilter<T>> extends AbstractRecordFilter<T> {

    private static final String DEFAULT_SOURCE_FIELD = "message";

    private static final Schema DEFAULT_COLUMN_TYPE = Schema.string();

    private static final String AUTO_GENERATED_COLUMN_NAME_PREFIX = "column";

    private DelimitedRowFilterConfig configs;

    private StructSchema schema;

    private String cachedHeaders;

    private final Map<Integer, TypedField> columnsTypesByIndex = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        this.configs = new DelimitedRowFilterConfig(configDef(), configs);

        if (isMandatoryConfigsMissing()) {
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            final String mandatory = joiner
                    .add(READER_AUTO_GENERATE_COLUMN_NAME_CONFIG)
                    .add(READER_EXTRACT_COLUMN_NAME_CONFIG)
                    .add(READER_FIELD_COLUMNS_CONFIG).toString();
            throw new ConfigException("At least one of those parameters should be configured " + mandatory);
        }

        this.schema = this.configs.schema();
        if (schema != null) {
            final List<TypedField> fields = schema.fieldsByIndex();
            IntStream.range(0, fields.size()).forEach(i -> columnsTypesByIndex.put(i, fields.get(i)));
        }
    }

    private boolean isMandatoryConfigsMissing() {
        return configs.schema() == null &&
               configs.extractColumnName() == null &&
               !configs.isAutoGenerateColumnNames();
    }

    public DelimitedRowFilterConfig filterConfig() {
        return configs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return DelimitedRowFilterConfig.configDef();
    }

    protected abstract String[] parseColumnsValues(final String line);

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        final String source = record.first(DEFAULT_SOURCE_FIELD).getString();

        String[] columnValues = parseColumnsValues(source);

        if (schema == null || isSchemaDynamic()) {
            inferSchemaFromRecord(record, columnValues.length);
        }

        if (schema != null && configs.extractColumnName() != null && shouldInferSchema(record)) {
            inferSchemaFromRecord(record, columnValues.length);
        }

        final TypedStruct struct = buildStructForFields(columnValues);
        return RecordsIterable.of(struct);
    }

    public boolean isSchemaDynamic() {
        // Schema SHOULD be inferred for each record when columns name are auto generate.
        // This rule is used to handle cases where records may have different number of columns.
        return configs.extractColumnName() == null &&
               configs.schema() == null &&
               configs.isAutoGenerateColumnNames();
    }

    private boolean shouldInferSchema(TypedStruct record) {
        if (cachedHeaders == null) {
            return false;
        }
        final String fieldName = configs.extractColumnName();
        String field = record.first(fieldName).getString();
        return cachedHeaders.length() == field.length() && !cachedHeaders.equals(field);
    }

    private void inferSchemaFromRecord(final TypedStruct record, int numColumns) {
        schema = Schema.struct();

        if (configs.extractColumnName() != null) {
            final String fieldName = configs.extractColumnName();
            String field = record.first(fieldName).getString();
            cachedHeaders = field;

            if (field == null) {
                throw new FilterException(
                    "Cannot find field for name '" + fieldName + "' to determine columns names"
                );
            }
            final List<String> columns = Arrays
                    .stream(parseColumnsValues(field))
                    .map(String::trim)
                    .collect(Collectors.toList());

            if (configs.isDuplicateColumnsAsArray()) {
                columns.stream()
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.<String>counting()))
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                        return e.getValue() > 1 ? Schema.array(DEFAULT_COLUMN_TYPE) : DEFAULT_COLUMN_TYPE;
                    }))
                    .forEach(schema::field);
            } else {
                columns.forEach(columnName -> schema.field(columnName, DEFAULT_COLUMN_TYPE));
            }
            IntStream.range(0, columns.size()).forEach(i -> columnsTypesByIndex.put(i, schema.field(columns.get(i))));
            return;
        }

        if (configs.isAutoGenerateColumnNames()) {
            for (int i = 0; i < numColumns; i++) {
                final String fieldName = AUTO_GENERATED_COLUMN_NAME_PREFIX + (i + 1);
                schema.field(fieldName, DEFAULT_COLUMN_TYPE);
                columnsTypesByIndex.put(i, schema.field(fieldName));
            }
            return;
        }

        throw new FilterException("Can't found valid configuration to determine schema for input value");
    }

    private TypedStruct buildStructForFields(final String[] fieldValues) {
        if (fieldValues.length > columnsTypesByIndex.size()) {
            throw new FilterException(
                "Error while reading delimited input row. Too large number of fields (" + fieldValues.length + ")"
            );
        }

        TypedStruct struct = TypedStruct.create();
        for (int i = 0; i < fieldValues.length; i++) {
            String fieldValue = fieldValues[i];
            if (configs.isTrimColumn()) {
                fieldValue = fieldValue.trim();
            }
            TypedField field = columnsTypesByIndex.get(i);
            final Type type = field.type();
            if (type == Type.ARRAY) {
                if (!struct.exists(field.name())) {
                    struct.put(field.name(), new ArrayList<>());
                }
                struct.getArray(field.name()).add(fieldValue); // it seems to be OK to use type conversion here
            } else {
                Object converted = StringUtils.isNotBlank(fieldValue) ? type.convert(fieldValue) : null;
                struct = struct.put(field.name(), type, converted);
            }
        }
        return struct;
    }
}