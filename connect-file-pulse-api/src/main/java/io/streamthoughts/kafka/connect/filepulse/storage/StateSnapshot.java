/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.storage;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class StateSnapshot<T> {

    public static <T> StateSnapshot<T> empty() {
        return new StateSnapshot<>(-1, Collections.emptyMap());
    }

    private final long offset;

    private final Map<String, T> states;

    /**
     * Creates a  new {@link StateSnapshot} instance.
     *
     * @param offset the startPosition of this snapshot.
     * @param states the state snapshot.
     */
    public StateSnapshot(final long offset, final Map<String, T> states) {
        Objects.requireNonNull(states, "states can't be null");
        this.offset = offset;
        this.states = states;
    }

    public boolean contains(final String key) {
        Objects.requireNonNull(states, "key can't be null");
        return states.containsKey(key);
    }

    public T getForKey(final String key) {
        Objects.requireNonNull(states, "key can't be null");
        return states.get(key);
    }

    public Map<String, T> states() {
        return states;
    }

    public long offset() {
        return offset;
    }
}
