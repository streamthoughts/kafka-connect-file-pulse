/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.json;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;

/**
 * Default interface to manage conversion from input JSON message to {@link TypedStruct} object.
 */
public interface JSONStructConverter {

    static JSONStructConverter createDefault() {
        return new DefaultJSONStructConverter();
    }

    /**
     * Gets a {@link TypedStruct} instance for the specified value.
     *
     * @param data  the json message to convert to {@link TypedStruct}.
     * @return      the new {@link TypedStruct} instance.
     */
    TypedValue readJson(final String data) throws Exception;
}
