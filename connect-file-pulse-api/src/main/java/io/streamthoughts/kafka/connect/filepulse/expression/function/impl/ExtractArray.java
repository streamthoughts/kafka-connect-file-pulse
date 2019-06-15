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

import io.streamthoughts.kafka.connect.filepulse.data.DataException;
import io.streamthoughts.kafka.connect.filepulse.data.TypeValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import io.streamthoughts.kafka.connect.filepulse.expression.function.MissingArgumentValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.SimpleArguments;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.List;

/**
 * Returns the element at the specified position in an array field.
 */
public class ExtractArray implements ExpressionFunction<SimpleArguments> {

    private static final String INDEX_ARG = "index";

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleArguments prepare(final TypeValue[] args) {
        if (args.length < 1) {
            return new SimpleArguments(new MissingArgumentValue(INDEX_ARG));
        }

        try {
            return new SimpleArguments(new ArgumentValue(INDEX_ARG, args[0].getInt()));
        } catch (DataException e) {
            return new SimpleArguments(new ArgumentValue(INDEX_ARG, args[0], "must be of type 'integer'"));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final SchemaAndValue value) {
        return value.schema().type().equals(Schema.Type.ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public SchemaAndValue apply(final SchemaAndValue data, final SimpleArguments args) {
        List<Object> array = (List<Object>) data.value();

        if (array != null) {
            Integer index = args.valueOf(INDEX_ARG);
            return new SchemaAndValue(data.schema().valueSchema(), array.get(index));
        }

        return null;
    }
}
