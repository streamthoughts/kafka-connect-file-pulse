/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.parser;

import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;

public class ScELParseCancellationException extends ExpressionException {

    /**
     * Creates a new {@link ExpressionException} instance.
     *
     * @param message the message to be used.
     */
    public ScELParseCancellationException(final String message) {
        super(message);
    }
}
