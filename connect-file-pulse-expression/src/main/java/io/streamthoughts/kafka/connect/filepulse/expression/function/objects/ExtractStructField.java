/*
 * Copyright 2022 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.expression.function.objects;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractExpressionFunctionInstance;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.EvaluatedExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import java.util.Objects;

/**
 * An {@link ExpressionFunction} to extract a field from {@link TypedStruct} object.
 */
public class ExtractStructField implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new FunctionInstance(syntax());
    }

    private String syntax() {
        return String.format("syntax %s(<struct_expr>, <path>", name());
    }

    public static final class FunctionInstance extends AbstractExpressionFunctionInstance {

        public static final int EXPECTED_NUM_ARGS = 2;
        private final String syntax;

        public FunctionInstance(final String syntax) {
            this.syntax = Objects.requireNonNull(syntax, "'syntax' cannot be null");
        }

        private String path;

        /**
         * {@inheritDoc}
         */
        @Override
        public Arguments prepare(final Expression[] args) {
            if (args.length > EXPECTED_NUM_ARGS) {
                throw new ExpressionException("Too many arguments: " + syntax);
            }
            if (args.length < EXPECTED_NUM_ARGS) {
                throw new ExpressionException("Missing required arguments: " + syntax);
            }

            path = ((ValueExpression)args[1]).value().getString();

            return Arguments.of("struct", args[0]);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypedValue invoke(final EvaluatedExecutionContext context) throws ExpressionException {
            TypedValue value = context.get(0);
            if (value.isNull()) {
                return TypedValue.none();
            }

            if (value.type() != Type.STRUCT) {
                throw new ExpressionException("Expected type STRUCT, was " + value.type());
            }

            return value.getStruct().find(path);
        }
    }
}
