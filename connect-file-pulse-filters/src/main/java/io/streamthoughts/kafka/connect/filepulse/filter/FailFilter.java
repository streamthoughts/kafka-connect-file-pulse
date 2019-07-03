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

import io.streamthoughts.kafka.connect.filepulse.config.FailFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.RegexExpressionParser;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

public class FailFilter extends AbstractRecordFilter<FailFilter> {

    private FailFilterConfig config;

    private Expression expression;

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return FailFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new FailFilterConfig(props);
        expression = new RegexExpressionParser().parseExpression(config.message());
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<FileInputData> apply(final FilterContext context,
                                                final FileInputData struct,
                                                final boolean hasNext) throws FilterException {
        if (condition.apply(context, struct)) {
            final SchemaAndValue value = expression.evaluate(context);
            throw new FilterException(value.value().toString());
        }
        return new RecordsIterable<>(struct);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final FilterContext context, final FileInputData record) {
        // We should always accept record to filter into the apply method.
        return true;
    }
}
