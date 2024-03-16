/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.storage;


/**
 * Generic interface for callbacks
 */
public interface Callback<V> {

    /**
     * Invoked upon completion of the operation.
     *
     * @param error the error that caused the operation to fail, or null if no error occurred
     * @param result the return value, or null if the operation failed
     */
    void onCompletion(Throwable error, V result);
}