/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;

public class FilterException extends ConnectFilePulseException {

    /**
     * Creates a new {@link FilterException} instance.
     * @param message   the error message.
     */
    public FilterException(final String message) {
        super(message);
    }

    /**
     * Creates a new {@link FilterException} instance.
     * @param message   the error message.
     * @param cause     the error cause.
     */
    public FilterException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
