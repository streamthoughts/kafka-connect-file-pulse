/*
 * Copyright 2019-2020 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
