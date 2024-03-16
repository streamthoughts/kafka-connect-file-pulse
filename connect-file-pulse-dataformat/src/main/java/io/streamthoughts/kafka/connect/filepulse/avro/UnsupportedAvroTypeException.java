/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.avro;

import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;

public class UnsupportedAvroTypeException extends ConnectFilePulseException {

    public UnsupportedAvroTypeException(final String message) {
        super(message);
    }

}
