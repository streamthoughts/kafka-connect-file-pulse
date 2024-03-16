/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
