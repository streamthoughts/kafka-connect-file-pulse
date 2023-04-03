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
package io.streamthoughts.kafka.connect.filepulse.data.merger;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import java.util.Set;

/**
 * Default interface which is used to merge two {@link TypedStruct} instance.
 */
public interface TypeValueMerger {

    /**
     * Method to merge two {@link TypedStruct} instances.
     *
     * @param left      the left {@link TypedStruct} to be merged.
     * @param right     the right {@link TypedStruct} to be merged.
     * @param overwrite the left field that must overwritten.
     *
     * @return          the new {@link TypedStruct} instance.
     */
    TypedStruct merge(final TypedStruct left,
                      final TypedStruct right,
                      final Set<String> overwrite);
}
