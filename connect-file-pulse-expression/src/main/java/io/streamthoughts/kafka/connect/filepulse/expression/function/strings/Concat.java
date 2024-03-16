/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Concat implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return (context, args) -> {
            String concat = StreamSupport
                .stream(args.spliterator(), false)
                .map(argument -> argument.evaluate(context))
                .filter(TypedValue::isNotNull)
                .map(TypedValue::getString)
                .collect(Collectors.joining("", "", ""));
            return TypedValue.string(concat);
        };
    }
}
