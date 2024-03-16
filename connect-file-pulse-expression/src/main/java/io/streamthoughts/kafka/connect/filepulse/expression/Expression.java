/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression;

public interface Expression {

    /**
     * Evaluates the variables defined into the given context.
     *
     * @param context   the expression context.
     * @return          the object resulting from the expression evaluation.
     */
    Object readValue(final EvaluationContext context);

    /**
     * Evaluates the variables defined into the given context.
     *
     * @param context       the expression context.
     * @param expectedType  the class of the expected type to return.
     * @param <T>           the expected type to return.
     * @return              the object resulting from the expression evaluation.
     */
    <T> T readValue(final EvaluationContext context, final Class<T> expectedType);

    void writeValue(final Object value, final EvaluationContext context);

    default boolean canWrite() {
        return true;
    }

    /**
     * Returns the originals string expression.
     * @return the string expression.
     */
    String originalExpression();
}