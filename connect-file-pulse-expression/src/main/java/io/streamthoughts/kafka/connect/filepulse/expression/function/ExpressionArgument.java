/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import java.util.Objects;

public class ExpressionArgument extends GenericArgument {

    /**
     * Creates a new {@link ExpressionArgument} instance.
     *
     * @param name          the argument name.
     * @param expression    the argument expression.
     */
    public ExpressionArgument(final String name,
                              final Expression expression) {
        super(name, Objects.requireNonNull(expression, "'expression should not be null"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue evaluate(final EvaluationContext context) {
        return ((Expression)value()).readValue(context, TypedValue.class);
    }
}
