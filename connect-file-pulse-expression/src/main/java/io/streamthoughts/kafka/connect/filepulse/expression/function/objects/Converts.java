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
package io.streamthoughts.kafka.connect.filepulse.expression.function.objects;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExecutionContext;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

/**
 * Simple function to convert a field into a new type.
 */
public class Converts implements ExpressionFunction {

    private static final String FIELD_ARG = "field_expr";
    private static final String TYPE_ARG = "type";

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return new Instance() {

            private String syntax() {
                return String.format("syntax %s(<%s>, <%s>)", name(), FIELD_ARG, TYPE_ARG);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Arguments prepare(final Expression[] args) {
                if (args.length < 2) {
                    throw new ExpressionException("Missing required arguments: " + syntax());
                }
                return Arguments.of(FIELD_ARG, args[0], TYPE_ARG, args[1]);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public TypedValue invoke(final ExecutionContext context) throws ExpressionException {
                final TypedValue value = context.get(FIELD_ARG);
                final TypedValue type  = context.get(TYPE_ARG);
                return value.as(Type.valueOf(type.value()));
            }
        };
    }
}