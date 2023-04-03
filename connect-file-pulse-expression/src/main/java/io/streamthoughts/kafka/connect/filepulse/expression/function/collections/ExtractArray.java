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
package io.streamthoughts.kafka.connect.filepulse.expression.function.collections;

import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Returns the element at the specified position in an array field.
 */
public class ExtractArray implements ExpressionFunction {

    private static final String ARRAY_ARG = "array";
    private static final String INDEX_ARG = "index";

    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionFunction.Instance get() {
        return new ExtractArrayInstance(name());
    }

    static class ExtractArrayInstance extends AbstractExpressionFunctionInstance {

        private int index;

        private final String name;

        ExtractArrayInstance(final String name) {
            this.name = Objects.requireNonNull(name, "'name' should not be null");
        }

        private String syntax() {
            return String.format("syntax %s(<%s>, <%s>)", name, ARRAY_ARG, INDEX_ARG);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Arguments prepare(final Expression[] args) {
            if (args.length < 2) {
                throw new ExpressionException("Missing arguments: " + syntax());
            }

            try {
                this.index = ((ValueExpression) args[1]).value().getInt();
                return Arguments.of(ARRAY_ARG, args[0], INDEX_ARG, args[1]);
            } catch (DataException e) {
                throw new ExpressionException("Invalid argument: '" + INDEX_ARG + "' must be of type 'integer'");
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
            final TypedValue value = context.get(0);

            if (value.type() != Type.ARRAY) {
                throw new ExpressionException("Expected type ARRAY, was " + value.type());
            }

            List<Object> list = new ArrayList<>(value.getArray());
            return TypedValue.any(list.get(index));
        }
    }
}
