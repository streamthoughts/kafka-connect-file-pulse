/*
 * Copyright 2019 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Schema;
import io.streamthoughts.kafka.connect.filepulse.data.StructSchema;
import io.streamthoughts.kafka.connect.filepulse.data.TypedField;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_EXTRACT_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_COLUMNS_CONFIG;

public class DelimitedRowFilter extends AbstractRecordFilter<DelimitedRowFilter> {

    private static final String DEFAULT_SOURCE_FIELD = "message";

    private static Schema DEFAULT_COLUMN_TYPE = Schema.string();

    private static final String AUTO_GENERATED_COLUMN_NAME_PREFIX = "column";

    private DelimitedRowFilterConfig configs;

    private StructSchema schema;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        this.configs = new DelimitedRowFilterConfig(configs);

        if (isMandatoryConfigsMissing()) {
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            final String mandatory = joiner
                .add(READER_AUTO_GENERATE_COLUMN_NAME_CONFIG)
                .add(READER_EXTRACT_COLUMN_NAME_CONFIG)
                .add(READER_FIELD_COLUMNS_CONFIG).toString();
            throw new ConfigException("At least one of those parameters should be configured " + mandatory);
        }

        this.schema = this.configs.schema();
    }

    private boolean isMandatoryConfigsMissing() {
        return configs.schema() == null &&
               configs.extractColumnName() == null &&
               !configs.isAutoGenerateColumnNames();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return DelimitedRowFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                final TypedStruct record,
                                                final boolean hasNext) throws FilterException {

        final String source = record.first(DEFAULT_SOURCE_FIELD).getString();

        String[] fieldValues = splitFields(source);
        final StructSchema schema = getSchema(record, fieldValues.length);
        final TypedStruct struct = buildStructForFields(fieldValues, schema);
        return RecordsIterable.of(struct);
    }

    private StructSchema getSchema(final TypedStruct record, int n) {
        if (schema != null) return schema;

        schema = Schema.struct();
        if (configs.extractColumnName() != null) {
            final String fieldName = configs.extractColumnName();
            String field = record.first(fieldName).getString();
            if (field == null) {
                throw new FilterException(
                    "Can't found field for name '" + fieldName + "' to determine columns names");
            }
            final String[] columns = splitFields(field);

            for (String column : columns) {
                schema.field(column, DEFAULT_COLUMN_TYPE);
            }
        } else if (configs.isAutoGenerateColumnNames()) {
                for (int i = 0; i < n; i++) {
                    schema.field(AUTO_GENERATED_COLUMN_NAME_PREFIX + (i + 1), DEFAULT_COLUMN_TYPE);
                }
        } else {
            throw new FilterException("Can't found valid configuration to determine schema for input value");
        }
        return schema;
    }

    private String[] splitFields(final String value) {
        return value.split(configs.delimiter());
    }

    private TypedStruct buildStructForFields(final String[] fieldValues, final StructSchema schema) {
        List<TypedField> fields = schema.fields();
        if (fieldValues.length > fields.size()) {
            throw new FilterException(
                "Error while reading delimited input row. Too large number of fields (" + fieldValues.length + ")");
        }
        TypedStruct struct = new TypedStruct();
        for (int i = 0; i < fieldValues.length; i++) {
            String fieldValue = fieldValues[i];
            if (configs.isTrimColumn()) {
                fieldValue = fieldValue.trim();
            }
            TypedField field = fields.get(i);
            struct = struct.put(field.name(), fieldValue);
        }
        return struct;
    }
}