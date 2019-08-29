/*
 * Copyright 2019 StreamThoughts.
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
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import io.streamthoughts.kafka.connect.filepulse.expression.function.TypedExpressionFunction;

public class StartsWith extends TypedExpressionFunction<String, SimpleArguments> {

    private static final String PREFIX_ARG = "prefix";

    /**
     * Creates a new {@link TypedExpressionFunction} instance.
     */
    public StartsWith() {
        super(Type.STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleArguments prepare(final TypedValue[] args) {
        if (args.length < 1) {
            return new SimpleArguments(new MissingArgumentValue(PREFIX_ARG));
        }
        return new SimpleArguments(new ArgumentValue(PREFIX_ARG, args[0].getString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue apply(final TypedValue field, final SimpleArguments args) {
        final String prefix = args.valueOf(PREFIX_ARG);
        final String value = field.value();
        final boolean prefixed = value.startsWith(prefix);
        return TypedValue.bool(prefixed);
    }
}
