/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.function;

import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.EvaluationContext;

public interface Argument {

    /**
     * @return the name of this argument.
     */
    String name();

    /**
     * @return the value of this argument.
     */
    Object value();

    default TypedValue evaluate(final EvaluationContext context) {
        throw new UnsupportedOperationException();
    }
}
