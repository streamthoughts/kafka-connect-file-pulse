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

import io.streamthoughts.kafka.connect.filepulse.config.ConvertFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.internal.SchemaUtils;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public class ConvertFilter extends AbstractRecordFilter<ConvertFilter> {

    private ConvertFilterConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new ConvertFilterConfig(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return ConvertFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData record,
                                                final boolean hasNext) {

        final Field field = record.value().schema().field(config.field());
        if (field != null) {
            Struct converted = convert(record.value(), field, config.type());
            return new RecordsIterable<>(new FileInputData(converted));
        } else if (!config.ignoreMissing()) {
            throw new FilterException("Cannot find field with name '" + config.field() + "'");
        }

        return new RecordsIterable<>(record);
    }

    private static Struct convert(final Struct struct, final Field source, final Type type) {

        final Schema prevSchema = struct.schema();
        final SchemaBuilder newSchemaBuilder = SchemaUtils.copySchemaBasics(prevSchema);

        newSchemaBuilder.field(source.name(), type.schema().optional());

        for (final Field field: prevSchema.fields()) {
            if (!source.name().equals(field.name())) {
                newSchemaBuilder.field(field.name(), field.schema());
            }
        }

        final Struct newStruct = new Struct(newSchemaBuilder);

        for (final Field field : prevSchema.fields()) {

            if (!source.name().equals(field.name())) {
                newStruct.put(field.name(), struct.get(field));
            } else {
                newStruct.put(field.name(), type.convert(struct.get(field)));
            }
        }

        return newStruct;
    }
}
