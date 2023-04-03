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
package io.streamthoughts.kafka.connect.filepulse.filter.condition;

import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers;
import io.streamthoughts.kafka.connect.filepulse.filter.FilterContext;
import io.streamthoughts.kafka.connect.filepulse.filter.FilterException;
import io.streamthoughts.kafka.connect.filepulse.filter.InternalFilterContext;
import java.util.Objects;

public class ExpressionFilterCondition implements FilterCondition {

    private final Expression expression;

    /**
     * Creates a new {@link ExpressionFilterCondition} instance.
     *
     * @param expression    the expression string.
     */
    public ExpressionFilterCondition(final String expression) {
        Objects.requireNonNull(expression, "expression can't be null");
        this.expression = ExpressionParsers.parseExpression(expression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean apply(final FilterContext context,
                         final TypedStruct record) throws FilterException {
        Objects.requireNonNull(context, "context can not be null");
        Objects.requireNonNull(record, "record can not be null");

        InternalFilterContext internalContext = (InternalFilterContext) context;
        internalContext.setValue(record);

        final StandardEvaluationContext ec = new StandardEvaluationContext(
                internalContext,
                context.variables());

        Object o = expression.readValue(ec);

        TypedValue value;
        if (o instanceof TypedValue) {
            value = (TypedValue)o;
        } else {
            value = TypedValue.any(o);
        }
        try {
            return value.getBool();
        } catch (DataException e) {
            throw new FilterException("Invalid condition function result type, expecting boolean value - got : " + o);
        }
    }
}
