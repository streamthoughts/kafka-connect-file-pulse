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

import io.streamthoughts.kafka.connect.filepulse.config.SplitFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData record,
                                                final boolean hasNext) throws FilterException {
        final Struct struct = maySplitFields(record.value());
        return new RecordsIterable<>(new FileInputData(struct));
    }

    private Struct maySplitFields(final Struct struct) {

        if (configs.split().isEmpty()) {
            return struct;
        }

        final Schema prevSchema = struct.schema();
        final SchemaBuilder newSchemaBuilder = SchemaUtils.copySchemaBasics(prevSchema);

        for (final String key : configs.split()) {
            final Field field = prevSchema.field(key);
            if (field != null) {
                SchemaBuilder optionalArray = SchemaBuilder.array(SchemaBuilder.string())
                                                           .optional()
                                                           .defaultValue(null);
                newSchemaBuilder.field(key, optionalArray);
            }
        }

        for (final Field field : prevSchema.fields()) {
            if (!configs.split().contains(field.name())) {
                newSchemaBuilder.field(field.name(), field.schema());
            }
        }

        final Struct newStruct = new Struct(newSchemaBuilder);

        for (final Field field : prevSchema.fields()) {
            final String sourceName = field.name();
            if (configs.split().contains(sourceName)) {
                String value = struct.getString(sourceName);
                List<String> array = Arrays.asList(value.split(configs.splitSeparator()));
                newStruct.put(sourceName, array);
            } else {
                newStruct.put(sourceName, struct.get(field));
            }
        }

        return newStruct;
    }
}
