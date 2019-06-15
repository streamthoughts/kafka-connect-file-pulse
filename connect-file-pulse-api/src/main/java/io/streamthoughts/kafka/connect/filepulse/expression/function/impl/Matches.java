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

import io.streamthoughts.kafka.connect.filepulse.data.TypeValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.regex.Pattern;

public class Matches implements ExpressionFunction<SimpleArguments> {

    private static final String PATTERN_ARG = "pattern";

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final SchemaAndValue value) {
        return value.schema().type().equals(Schema.Type.STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleArguments prepare(final TypeValue[] args) {
        if (args.length < 1) {
            return new SimpleArguments(new MissingArgumentValue(PATTERN_ARG));
        }
        String pattern = args[0].getString();
        return new SimpleArguments(new ArgumentValue(PATTERN_ARG,  Pattern.compile(pattern)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue apply(final SchemaAndValue field, final SimpleArguments args) {
        final Pattern pattern = args.valueOf(PATTERN_ARG);
        final boolean matches = pattern.matcher((String)field.value()).matches();
        return new SchemaAndValue(Schema.BOOLEAN_SCHEMA, matches);
    }
}
