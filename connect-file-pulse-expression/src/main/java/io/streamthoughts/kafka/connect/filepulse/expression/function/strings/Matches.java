/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import java.util.Objects;
import java.util.regex.Pattern;

public class Matches implements ExpressionFunction {

    private static final String FIELD_ARG = "field_expr";
    private static final String PATTERN_ARG = "pattern";

    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionFunction.Instance get() {
        return new Instance(name());
    }

    static class Instance extends AbstractExpressionFunctionInstance {

        private Pattern pattern;

        private final String name;

        public Instance(final String name) {
            this.name = Objects.requireNonNull(name, "'name' should not be null");
        }

        private String syntax() {
            return String.format("syntax %s(<%s>, <%s>)", name, FIELD_ARG, PATTERN_ARG);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Arguments prepare(final Expression[] args) {
            if (args.length > 2) {
                throw new ExpressionException("Too many arguments: " + syntax());
            }
            if (args.length < 2) {
                throw new ExpressionException("Missing required arguments: " + syntax());
            }

            this.pattern =  Pattern.compile(((ValueExpression)args[1]).value().getString());
            return Arguments.of(FIELD_ARG, args[0]);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
            final String value = context.get(0).value();
            return TypedValue.bool(pattern.matcher(value).matches());
        }
    }
}
