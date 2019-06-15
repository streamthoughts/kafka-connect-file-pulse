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

import io.streamthoughts.kafka.connect.filepulse.config.AppendFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.RegexExpressionParser;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AppendFilter extends AbstractMergeRecordFilter<AppendFilter> {

    private AppendFilterConfig config;

    private List<Expression> expressions;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new AppendFilterConfig(props);
        expressions = Collections.singletonList(new RegexExpressionParser().parseExpression(config.value()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return AppendFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordsIterable<FileInputData> apply(final FilterContext context, final FileInputData record) {

        Struct struct = null;

        for (Expression expression : expressions) {
            final SchemaAndValue schemaAndValue = expression.evaluate(context);
            if (schemaAndValue != null) {
                if (struct == null) {
                    Schema valueSchema = expressions.size() > 1 ?
                            SchemaBuilder.array(schemaAndValue.schema()).optional() :
                            schemaAndValue.schema();
                    SchemaBuilder sb = SchemaBuilder.struct().field(config.field(), valueSchema);
                    struct = new Struct(sb);
                }
                Object value = schemaAndValue.value();
                if (expressions.size() > 1) {
                    List<Object> o = struct.getArray(config.field());
                    if (o == null) {
                        o = new ArrayList<>();
                    }
                    o.add(value);
                    value = o;
                }
                struct.put(config.field(), value);
            }
        }
        return new RecordsIterable<>(new FileInputData(struct));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        if (config.overwrite()) {
            return Collections.singleton(config.field());
        }
        return Collections.emptySet();
    }
}