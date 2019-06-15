/*
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
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_AUTO_GENERATE_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_EXTRACT_COLUMN_NAME_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.config.DelimitedRowFilterConfig.READER_FIELD_COLUMNS_CONFIG;

public class DelimitedRowFilter extends AbstractRecordFilter<DelimitedRowFilter> {

    private static SchemaBuilder DEFAULT_COLUMN_TYPE = SchemaBuilder.string().optional().defaultValue(null);

    private static final String AUTO_GENERATED_COLUMN_NAME_PREFIX = "column";

    private DelimitedRowFilterConfig configs;

    private Schema schema;

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
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData record,
                                                final boolean hasNext) {

        final String source = record.getFirstValueForField(FileInputData.DEFAULT_MESSAGE_FIELD);

        String[] fieldValues = splitFields(source);
        final Schema schema = getSchema(record, fieldValues.length);
        final Struct struct = buildStructForFields(fieldValues, schema);
        return new RecordsIterable<>(new FileInputData(struct));
    }

    private Schema getSchema(final FileInputData record, int n) {
        if (schema != null) return schema;

        if (configs.extractColumnName() != null) {
            final String fieldName = configs.extractColumnName();
            String field = record.getFirstValueForField(fieldName);
            if (field == null) {
                throw new ConnectException(
                    "Can't found field for name '" + fieldName + "' to determine columns names");
            }
            final String[] columns = splitFields(field);
            SchemaBuilder sb = SchemaBuilder.struct();
            for (String column : columns) {
                sb = sb.field(column, DEFAULT_COLUMN_TYPE);
            }
            schema = sb.build();
        } else if (configs.isAutoGenerateColumnNames()) {
                SchemaBuilder sb = SchemaBuilder.struct();
                for (int i = 0; i < n; i++) {
                    sb = sb.field(AUTO_GENERATED_COLUMN_NAME_PREFIX + (i + 1), DEFAULT_COLUMN_TYPE);
                }
                schema = sb.build();
        } else {
            throw new ConnectException("Can't found valid configuration to determine schema for input data");
        }
        return schema;
    }

    private String[] splitFields(final String value) {
        return value.split(configs.delimiter());
    }

    private Struct buildStructForFields(final String[] fieldValues, final Schema schema) {
        List<Field> fieldsSchema = schema.fields();
        if (fieldValues.length > fieldsSchema.size()) {
            throw new ReaderException(
                "Error while reading delimited input row. Too large number of fields (" + fieldValues.length + ")");
        }
        Struct struct = new Struct(schema);
        for (int i = 0; i < fieldValues.length; i++) {
            String fieldValue = fieldValues[i];
            if (configs.isTrimColumn()) {
                fieldValue = fieldValue.trim();
            }
            struct = struct.put(fieldsSchema.get(i), fieldValue);
        }
        return struct;
    }
}