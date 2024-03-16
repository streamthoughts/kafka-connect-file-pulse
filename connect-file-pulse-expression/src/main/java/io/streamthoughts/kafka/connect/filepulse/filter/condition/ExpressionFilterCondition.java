/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
