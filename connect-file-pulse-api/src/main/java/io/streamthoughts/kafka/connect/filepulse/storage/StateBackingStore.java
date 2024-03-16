/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.storage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface StateBackingStore<T> extends AutoCloseable {

    /**
     * Start dependent services (if needed)
     */
    void start();

    /**
     * Stop dependent services (if needed)
     */
    void stop();

    /**
     * Checks if the store is started.
     */
    boolean isStarted();

    /**
     * Get a snapshot of the current state.
     *
     * @return the {@link StateSnapshot}.
     */
    StateSnapshot<T> snapshot();

    /**
     * Check if the store has state for a specified name.
     *
     * @param name name of the state.
     * @return true     if the backing store contains value for the state
     */
    boolean contains(final String name);

    /**
     * Update asynchronously the state for the specified name.
     *
     * @param name name of the connector.
     * @param state the state value.
     */
    void putAsync(final String name, final T state);

    /**
     * Update the state for the specified name.
     *
     * @param name name of the connector.
     * @param state the state value.
     */
    void put(final String name, final T state);

    /**
     * Remove the state for a specified name.
     *
     * @param name name of the state
     */
    void remove(final String name);

    /**
     * Remove state for a specified name.
     *
     * @param name name of the state
     */
    void removeAsync(final String name);

    /**
     * Refresh the backing store. This forces the store to ensure that it has the latest
     * configs that have been written.
     *
     * @param timeout max time to wait for the refresh to complete
     * @param unit unit of timeout
     * @throws TimeoutException if the timeout expires before the refresh has completed
     */
    void refresh(final long timeout, final TimeUnit unit) throws TimeoutException;

    /**
     * Set an update listener to instance notifications when there are configDef/target state
     * changes.
     *
     * @param listener non-null listener
     */
    void setUpdateListener(final UpdateListener<T> listener);

    /**
     * {@inheritDoc}
     */
    @Override
    default void close() {
        stop();
    }

    interface UpdateListener<T> {

        /**
         * Invoked when a state has been removed
         *
         * @param state name of the state
         */
        void onStateRemove(final String state);

        /**
         * Invoked when a state has been updated.
         *
         * @param state name of the state
         * @param value value of the state.
         */
        void onStateUpdate(final String state, final T value);
    }

}