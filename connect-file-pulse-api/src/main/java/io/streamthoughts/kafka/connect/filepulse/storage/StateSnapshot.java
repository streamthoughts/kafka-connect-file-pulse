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
