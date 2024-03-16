/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractTransformExpressionFunction;

public class IsEmpty extends AbstractTransformExpressionFunction {


    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue transform(final TypedValue value) {
        if (value.type() != Type.ARRAY && value.type() != Type.STRING) {
            throw new ExpressionException("Expected type [ARRAY|STRING], was " + value.type());
        }

        int size = value.type() == Type.ARRAY ? value.getArray().size() : value.getString().length();
        return TypedValue.bool(size == 0);
    }
}
