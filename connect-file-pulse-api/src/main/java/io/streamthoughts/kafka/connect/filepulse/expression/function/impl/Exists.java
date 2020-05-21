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

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;

public class Exists implements ExpressionFunction<SimpleArguments> {

    private static final String FIELD_ARG = "field";

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final TypedValue value) {
        return value.type() == Type.STRUCT || value.type() == Type.NULL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleArguments prepare(final TypedValue[] args) {
        if (args.length < 1) {
            return new SimpleArguments(new MissingArgumentValue(FIELD_ARG));
        }
        return new SimpleArguments(new ArgumentValue(FIELD_ARG, args[0].getString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue apply(final TypedValue field, final SimpleArguments args) {

        if (field.type() == Type.NULL) return TypedValue.bool(false);

        final TypedStruct struct = field.getStruct();
        final String arg = args.valueOf(FIELD_ARG);
        final boolean exists = struct.has(arg);
        return TypedValue.bool(exists);
    }
}
