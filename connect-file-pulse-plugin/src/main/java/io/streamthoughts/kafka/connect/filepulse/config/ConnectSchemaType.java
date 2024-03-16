/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import java.util.Arrays;
import java.util.Locale;

public enum ConnectSchemaType {
    INVALID,
    CONNECT,
    AVRO;

    public static ConnectSchemaType getForNameIgnoreCase(final String str) {
        return Arrays.stream(ConnectSchemaType.values())
                .filter(e -> e.name().equals(str.toUpperCase(Locale.ROOT)))
                .findFirst()
                .orElse(ConnectSchemaType.INVALID);
    }
}
