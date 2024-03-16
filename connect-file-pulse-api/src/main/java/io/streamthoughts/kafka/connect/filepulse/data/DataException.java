/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.data;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;

public final class DataException extends ConnectFilePulseException {

    /**
     * Creates a new {@link DataException} instance.
     *
     * @param message the error message.
     */
    public DataException(final String message) {
        super(message);
    }

    /**
     * Creates a new {@link DataException} instance.
     *
     * @param message the error message.
     * @param cause   the error cause.
     */
    public DataException(final String message, final Throwable cause) {
        super(message, cause);
    }
}