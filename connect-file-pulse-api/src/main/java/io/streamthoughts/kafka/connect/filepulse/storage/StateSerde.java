/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.storage;

/**
 *
 */
public interface StateSerde<T> {

    byte[] serialize(final T state);

    T deserialize(final byte[] configs);
}
