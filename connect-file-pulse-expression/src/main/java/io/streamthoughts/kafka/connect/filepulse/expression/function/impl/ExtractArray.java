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
package io.streamthoughts.kafka.connect.filepulse.expression.function.impl;

import io.streamthoughts.kafka.connect.filepulse.data.ArraySchema;
import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.ValueExpression;
import io.streamthoughts.kafka.connect.filepulse.expression.function.Arguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.GenericArgument;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;

import java.util.ArrayList;
import java.util.List;

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
    public Arguments prepare(final Expression[] args) {
        if (args.length < 2) {
            return Arguments.of(
                new MissingArgumentValue(ARRAY_ARG),
                new MissingArgumentValue(INDEX_ARG)
            );
        }

        try {
            return Arguments.of(
                new ExpressionArgument(ARRAY_ARG, args[0]),
                new GenericArgument<>(INDEX_ARG, ((ValueExpression)args[1]).value().getInt())
            );
        } catch (DataException e) {
            return Arguments.of(
                new ExpressionArgument(ARRAY_ARG, args[0]),
                new GenericArgument<>(INDEX_ARG, (ValueExpression)args[1], "must be of type 'integer'")
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Arguments<GenericArgument> validate(final Arguments<GenericArgument> args) {
        GenericArgument argument = args.get(0);
        TypedValue value = (TypedValue) argument.value();
        if (value.type() != Type.ARRAY) {
            argument.addErrorMessage("Expected type ARRAY, was " + value.type());
        }
        return args;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue apply(final Arguments<GenericArgument> args) {
        TypedValue array = args.valueOf(ARRAY_ARG);
        List<Object> list = new ArrayList<>(array.getArray());
        return TypedValue.of(list.get(args.valueOf(INDEX_ARG)), ((ArraySchema) array.schema()).valueSchema());
    }
}
