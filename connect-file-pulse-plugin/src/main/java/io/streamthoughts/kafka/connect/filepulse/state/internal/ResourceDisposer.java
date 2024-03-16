/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.state.internal;

/**
 * A {@code MemoryResourceDisposer} can be used to dispose a shared
 * resource after it is not used any more.
 *
 * @see io.streamthoughts.azkarra.commons.rocksdb.internal.OpaqueMemoryResource
 * @param <E>   the exception type.
 */

@FunctionalInterface
public interface ResourceDisposer<E extends Throwable> {

    /**
     * Release the memory shared resource.
     *
     * @throws E    if the resource cannot be disposed.
     */
    void dispose() throws E;
}
