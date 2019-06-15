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
package io.streamthoughts.kafka.connect.filepulse.expression.function.impl;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypeValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import org.apache.kafka.connect.data.SchemaAndValue;

/**
 * Replace a null value with a non-null value.
 */
public class Nlv implements ExpressionFunction<SimpleArguments> {

    private static final String DEFAULT_ARG = "default";

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleArguments prepare(final TypeValue[] args) {
        if (args.length < 1) {
            return new SimpleArguments(new MissingArgumentValue(DEFAULT_ARG));
        }
        return new SimpleArguments(new ArgumentValue(DEFAULT_ARG, args[0]));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue apply(final SchemaAndValue data, final SimpleArguments args) {
        if (data.value() == null) {
            Object defaultVal = args.valueOf(DEFAULT_ARG);
            return new SchemaAndValue(data.schema(), Type.fromSchemaType(data.schema().type()).convert(defaultVal));
        }
        return data;
    }
}