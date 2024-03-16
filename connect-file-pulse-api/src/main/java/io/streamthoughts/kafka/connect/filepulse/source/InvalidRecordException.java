/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;

public class InvalidRecordException extends ConnectFilePulseException {

    /**
     * Creates a new {@link InvalidRecordException} instance.
     * @param message   the error message.
     */
    public InvalidRecordException(final String message) {
        super(message);
    }
}
