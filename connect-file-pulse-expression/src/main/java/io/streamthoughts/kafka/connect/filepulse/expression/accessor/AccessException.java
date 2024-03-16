/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.accessor;

import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;

public class AccessException extends ExpressionException {

    /**
     * Creates a new {@link AccessException} instance.
     *
     * @param message   the cause message.
     */
    public AccessException(final String message) {
        super(message);
    }
}
