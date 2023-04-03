/*
 * Copyright 2019-2020 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.config.AppendFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.PropertyExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;

public class AppendFilter extends AbstractMergeRecordFilter<AppendFilter> {

    private AppendFilterConfig config;

    private List<Expression> values;
    private Expression fieldExpression;

    protected String target;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new AppendFilterConfig(props);

        // currently, multiple expressions is not supported
        values = Collections.singletonList(ExpressionParsers.parseExpression(config.value()));

        fieldExpression = config.field();
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
        internalContext.setValue(record);

        StandardEvaluationContext readEvaluationContext = new StandardEvaluationContext(
                internalContext,
                internalContext.variables()
        );

        final Expression writeExpression = mayEvaluateWriteExpression(readEvaluationContext);

        TypedStruct target = TypedStruct.create();
        for (Iterator<Expression> iterator = values.iterator(); iterator.hasNext() && target != null; ) {
            Expression expression = iterator.next();

            internalContext.setValue(record); // set the readable value before evaluating the expression
            final Object value = expression.readValue(readEvaluationContext);


            final StandardEvaluationContext writeEvaluationContext = new StandardEvaluationContext(
                    internalContext,
                    internalContext.variables()
            );
            internalContext.setValue(target); // set the writable value before evaluating the expression
            writeExpression.writeValue(value, writeEvaluationContext);

            // We must retrieve the target-value from the context which be set to null by the expression.
            target = internalContext.value();
        }

        return RecordsIterable.of(target);
    }

    private Expression mayEvaluateWriteExpression(final StandardEvaluationContext evaluationContext) {
        Expression expression = fieldExpression;
        if (!fieldExpression.canWrite()) {
            final String evaluated = fieldExpression.readValue(evaluationContext, String.class);
            if (evaluated == null) {
                throw new FilterException("Invalid value for property 'field'. Evaluation of expression '"
                    + fieldExpression.originalExpression() + " 'returns 'null'.");
            }
            expression = ExpressionParsers.parseExpression(evaluated);
        }

        target = ((PropertyExpression)expression).getAttribute();
        return expression;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<String> overwrite() {
        if (config.isOverwritten() && target != null) {
            return Collections.singleton(target);
        }
        return Collections.emptySet();
    }
}