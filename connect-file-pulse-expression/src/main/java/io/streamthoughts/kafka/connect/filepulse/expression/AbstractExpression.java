/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public abstract class AbstractExpression implements Expression {

    private final String originalExpression;

    /**
     * Creates a new {@link AbstractExpression} instance.
     *
     * @param originalExpression    the original string expression.
     */
    public AbstractExpression(final String originalExpression) {
        this.originalExpression = requireNonNull(originalExpression, "originalExpression cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractExpression)) return false;
        AbstractExpression that = (AbstractExpression) o;
        return Objects.equals(originalExpression, that.originalExpression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(originalExpression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String originalExpression() {
        return originalExpression;
    }
}
