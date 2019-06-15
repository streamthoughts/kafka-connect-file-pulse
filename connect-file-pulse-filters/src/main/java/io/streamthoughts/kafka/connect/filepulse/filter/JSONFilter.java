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

import io.streamthoughts.kafka.connect.filepulse.config.JSONFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.json.DefaultJSONStructConverter;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import java.util.Map;
import java.util.Set;

public class JSONFilter extends AbstractMergeRecordFilter<JSONFilter> {

    private final DefaultJSONStructConverter converter = new DefaultJSONStructConverter();

    private JSONFilterConfig configs;

    private String source;

    private String target;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        configs = new JSONFilterConfig(props);
        source = configs.source();
        target = configs.target();
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
    protected RecordsIterable<FileInputData> apply(final FilterContext context, final FileInputData record) {
        final String value = record.value().getString(source);

        if (value == null) return null;

        final FileInputData json = converter.readJson(value);

        if (target != null) {
            Schema schema = SchemaBuilder.struct().field(target, json.schema()).build();
            return new RecordsIterable<>(
                    new FileInputData(new Struct(schema).put(target, json.value()))
            );
        }
        return new RecordsIterable<>(json);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        return configs.overwrite();
    }
}