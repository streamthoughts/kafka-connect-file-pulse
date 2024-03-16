/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.objects;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.AbstractTransformExpressionFunction;

public class IsNull extends AbstractTransformExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public TypedValue transform(final TypedValue value) {
        return TypedValue.bool(value == null || value.isNull());
    }
}
