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

import io.streamthoughts.kafka.connect.filepulse.config.JoinFilterConfig;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.regex.RegexExpressionParser;
import io.streamthoughts.kafka.connect.filepulse.reader.RecordsIterable;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class JoinFilter extends AbstractRecordFilter<JoinFilter> {

    private static final String DEFAULT_ROOT_OBJECT = "value";

    private JoinFilterConfig config;

    private RegexExpressionParser parser;

    private Expression fieldExpression;

    private Expression targetExpression;

    private boolean mustEvaluateTargetExpression = true;

    private boolean mustEvaluateFieldExpression = true;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        super.configure(props);
        config = new JoinFilterConfig(props);

        parser = new RegexExpressionParser();

        // Parse expression while supporting substitution
        fieldExpression = parser.parseExpression(config.field(), DEFAULT_ROOT_OBJECT);
        // Check whether the field expression can be pre-evaluated (i.e : is not a substitution expression).
        if (fieldExpression instanceof ValueExpression) {
            fieldExpression = mayEvaluateFieldExpression(new StandardEvaluationContext(new Object()));
            mustEvaluateFieldExpression = false;
        }

        if (config.target() == null)  {
            targetExpression = fieldExpression;
            mustEvaluateTargetExpression = mustEvaluateFieldExpression;
        } else {
            targetExpression = parser.parseExpression(config.target(), DEFAULT_ROOT_OBJECT);
            // Check whether the target expression can be pre-evaluated (i.e : is not a substitution expression).
            if (targetExpression instanceof ValueExpression) {
                targetExpression = mayEvaluateTargetExpression(new StandardEvaluationContext(new Object()));
                mustEvaluateTargetExpression = false;
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef configDef() {
        return JoinFilterConfig.configDef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordsIterable<TypedStruct> apply(final FilterContext context,
                                              final TypedStruct record,
                                              final boolean hasNext) throws FilterException {

        InternalFilterContext internalContext = (InternalFilterContext) context;
        internalContext.setValue(record);

        StandardEvaluationContext evaluationContext = new StandardEvaluationContext(
                internalContext,
                internalContext.variables()
        );

        final Expression field = mayEvaluateFieldExpression(evaluationContext);
        final Collection<?> array = field.readValue(evaluationContext, Collection.class);

        String joined = null;
        if (array != null) {
            joined = array.stream()
                .map(Object::toString)
                .collect(Collectors.joining(config.separator()));
        }

        final Expression target = mayEvaluateTargetExpression(evaluationContext);
        target.writeValue(joined, evaluationContext);

        return RecordsIterable.of(record);
    }

    private Expression mayEvaluateTargetExpression(final StandardEvaluationContext evaluationContext) {
        if (mustEvaluateTargetExpression) {
            final String evaluated = targetExpression.readValue(evaluationContext, String.class);
            return parser.parseExpression(evaluated, DEFAULT_ROOT_OBJECT, false);
        }
        return targetExpression;
    }

    private Expression mayEvaluateFieldExpression(final StandardEvaluationContext evaluationContext) {
        if (mustEvaluateFieldExpression) {
            final String evaluated = fieldExpression.readValue(evaluationContext, String.class);
            return parser.parseExpression(evaluated, DEFAULT_ROOT_OBJECT, false);
        }
        return fieldExpression;
    }
}
