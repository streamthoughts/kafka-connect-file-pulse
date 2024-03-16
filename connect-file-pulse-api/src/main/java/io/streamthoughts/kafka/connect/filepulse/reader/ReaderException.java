/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;

/**
 * Interface to manage exceptions thrown by {@link FileInputReader} implementations.
 */
public class ReaderException extends ConnectFilePulseException {

    public ReaderException(final Throwable cause) {
        super(cause);
    }

    public ReaderException(final String message) {
        super(message);
    }

    public ReaderException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
