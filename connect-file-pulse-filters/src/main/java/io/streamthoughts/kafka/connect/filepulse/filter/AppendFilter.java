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
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.regex.RegexExpressionParser;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AppendFilter extends AbstractMergeRecordFilter<AppendFilter> {

    private static final String DEFAULT_ROOT_OBJECT = "value";

    private AppendFilterConfig config;

    private List<Expression> values;
    private Expression target;

    private RegexExpressionParser parser;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new AppendFilterConfig(props);

        parser = new RegexExpressionParser();
        // currently, multiple expressions is not supported
        values = Collections.singletonList(parser.parseExpression(config.value(), DEFAULT_ROOT_OBJECT, true));
        target = parser.parseExpression(config.field());
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
    protected RecordsIterable<TypedStruct> apply(final FilterContext context,
                                                 final TypedStruct record) throws FilterException {

        InternalFilterContext internalContext = (InternalFilterContext) context;

        StandardEvaluationContext readEvaluationContext = new StandardEvaluationContext(
                internalContext,
                internalContext.variables()
        );

        final String evaluatedTarget = target.readValue(readEvaluationContext, String.class);
        final Expression targetExpression = parser.parseExpression(evaluatedTarget, DEFAULT_ROOT_OBJECT, false);

        final TypedStruct target = TypedStruct.struct();
        for (final Expression expression : values) {

            internalContext.setValue(record);
            final Object typed = expression.readValue(readEvaluationContext);

            if (typed != null) {
                internalContext.setValue(target);

                final StandardEvaluationContext writeEvaluationContext = new StandardEvaluationContext(
                    internalContext,
                    internalContext.variables()
                );
                targetExpression.writeValue(typed, writeEvaluationContext);
            }
        }
        return RecordsIterable.of(target);
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