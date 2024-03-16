/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.conditions;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import java.math.BigDecimal;

public class LessThan implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "lt";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new FunctionInstance();
    }

    private static final class FunctionInstance extends AbstractExpressionFunctionInstance {
        /**
         * {@inheritDoc}
         */
        @Override
        public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
            final BigDecimal value1 = new BigDecimal(context.get(0).getString());
            final BigDecimal value2 = new BigDecimal(context.get(1).getString());
            final int i = value1.compareTo(value2);
            return TypedValue.bool(i < 0);
        }
    }
}
