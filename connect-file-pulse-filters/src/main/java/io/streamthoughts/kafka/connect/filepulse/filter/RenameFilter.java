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

import io.streamthoughts.kafka.connect.filepulse.config.RenameFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public class RenameFilter extends AbstractRecordFilter<RenameFilter> {

    private RenameFilterConfig configs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new RenameFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return RenameFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData record,
                                                final boolean hasNext) {

        if (record.has(configs.field())) {
            Struct renamed = rename(record, record.field(configs.field()), configs.target());
            return new RecordsIterable<>(new FileInputData(renamed));
        } else if (!configs.ignoreMissing()) {
            throw new FilterException("Cannot find field with name '" + configs.field() + "'");
        }

        return new RecordsIterable<>(record);
    }

    private Struct rename(final FileInputData record,
                          final Field source,
                          final String target) {


        final Schema prevSchema = record.schema();
        final SchemaBuilder newSchemaBuilder = SchemaUtils.copySchemaBasics(prevSchema);

        newSchemaBuilder.field(target, source.schema());

        final String sourceName = source.name();
        for (final Field field : prevSchema.fields()) {
            if (!sourceName.equals(field.name())) {
                newSchemaBuilder.field(field.name(), field.schema());
            }
        }

        final Struct newStruct = new Struct(newSchemaBuilder);

        for (final Field field : prevSchema.fields()) {
            final String targetName = sourceName.equals(field.name()) ? target : sourceName;
            newStruct.put(targetName, record.get(field.name()));
        }

        return newStruct;
    }
}
