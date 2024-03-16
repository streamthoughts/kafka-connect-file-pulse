/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function.datetime;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.function.ExpressionFunction;

/**
 * Function to return the current Unix timestamp in seconds.
 */
public class UnixTimestamp implements ExpressionFunction {

    /**
     * {@inheritDoc}
     */
    @Override
    public Instance get() {
        return (context, args) -> TypedValue.int64(System.currentTimeMillis());
    }
}
