/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.state.internal;

/**
 * A {@code ResourceInitializer} is used to initialize a new resource.
 *
 * @param <T>   the resource type.
 */
@FunctionalInterface
public interface ResourceInitializer<T> {

    /**
     * Creates a new resource.
     *
     * @return  the resource.
     *
     * @throws Exception if the resource cannot be created/allocated.
     */
    T apply() throws Exception;
}
