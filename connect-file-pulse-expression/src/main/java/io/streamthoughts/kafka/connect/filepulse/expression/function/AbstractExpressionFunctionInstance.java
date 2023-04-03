/*
 * Copyright 2021 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import java.util.stream.IntStream;

public abstract class AbstractExpressionFunctionInstance implements ExpressionFunction.Instance {

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue invoke(final EvaluationContext context,
                             final Arguments arguments) throws ExpressionException {

        final EvaluatedExecutionContext executionContext = new EvaluatedExecutionContext();

        IntStream.range(0, arguments.size()).forEachOrdered(i -> {
            final Argument argument = arguments.get(i);
            final TypedValue value = argument.evaluate(context);
            executionContext.addArgument(argument.name(), i, value);
        });

        return invoke(executionContext);
    }

    /**
     * Executes the function with the specific context.
     *
     * @param context the {@link EvaluatedExecutionContext}.
     * @return the function result.
     * @throws ExpressionException if the function execution failed.
     */
    public abstract TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException;
}
