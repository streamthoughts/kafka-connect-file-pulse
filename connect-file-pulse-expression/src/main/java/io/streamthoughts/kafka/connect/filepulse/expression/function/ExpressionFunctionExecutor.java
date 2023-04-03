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
