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
