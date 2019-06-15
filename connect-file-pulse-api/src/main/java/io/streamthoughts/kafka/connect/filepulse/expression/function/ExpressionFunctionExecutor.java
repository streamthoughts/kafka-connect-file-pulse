/*
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

import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Objects;

public class ExpressionFunctionExecutor {

    private final String name;

    private final ExpressionFunction<Arguments> function;

    private final Arguments arguments;

    /**
     * Creates a new {@link ExpressionFunctionExecutor} instance.
     *
     * @param name          the function name;
     * @param function      the function.
     * @param arguments     the function arguments.
     */
    ExpressionFunctionExecutor(final String name,
                               final ExpressionFunction<Arguments> function,
                               final Arguments arguments) {
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(function, "function cannot be null");
        Objects.requireNonNull(arguments, "arguments cannot be null");
        this.name = name;
        this.function = function;
        this.arguments = arguments;
    }

    public SchemaAndValue execute(final SchemaAndValue record) {
        if (function.accept(record)) {
            return function.apply(record, arguments);
        } else {
            throw new ExpressionException(
                String.format(
                    "Cannot applied method '%s' on record with schema=%s, value=%s",
                    name,
                    record.schema(),
                    record.value()));
        }

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
