/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.storage;

/**
 * A state store supplier that can create one StateBackingStore instance.
 *
 * @param <T> type of state-store.
 */
@FunctionalInterface
public interface StateStoreProvider<T> {

    /**
     * Returns a new {@link StateBackingStore} instance.
     * @return  a new {@link StateBackingStore} instance.
     */
    StateBackingStore<T> get();
}
