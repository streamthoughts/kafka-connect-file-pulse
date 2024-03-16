/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.expression.converter;

import io.streamthoughts.kafka.connect.filepulse.expression.ExpressionException;

class ConversionException extends ExpressionException {

    /**
     * Creates a new {@link ConversionException} instance.
     *
     * @param message  sthe cause message.
     */
    ConversionException(String message) {
        super(message);
    }
}
