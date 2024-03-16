/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;
import java.util.Objects;

public class ExpressionFunctionExecutor {

    private final String name;

    private final ExpressionFunction.Instance instance;

    private final Arguments arguments;

    /**
     * Creates a new {@link ExpressionFunctionExecutor} instance.
     *
     * @param name          the function name;
     * @param instance      the function instance.
     * @param arguments     the function arguments.
     */
    ExpressionFunctionExecutor(final String name,
                               final ExpressionFunction.Instance instance,
                               final Arguments arguments) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.instance = Objects.requireNonNull(instance, "instance cannot be null");
        this.arguments = Objects.requireNonNull(arguments, "arguments cannot be null");
    }

    public TypedValue execute(final EvaluationContext context) {
        return instance.invoke(context, arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExpressionFunctionExecutor)) return false;
        ExpressionFunctionExecutor that = (ExpressionFunctionExecutor) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(instance, that.instance) &&
                Objects.equals(arguments, that.arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" +
                "name='" + name + '\'' +
                ", arguments=" + arguments +
                ']';
    }
}
