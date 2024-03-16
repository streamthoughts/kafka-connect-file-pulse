/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.errors;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * ConnectFilePulseException is the top-level error type generated by Connect File Pulse plugin.
 */
public class ConnectFilePulseException extends ConnectException {

    public ConnectFilePulseException(final String s) {
        super(s);
    }

    public ConnectFilePulseException(final String s, final Throwable throwable) {
        super(s, throwable);
    }

    public ConnectFilePulseException(final Throwable throwable) {
        super(throwable);
    }
}
