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
package io.streamthoughts.kafka.connect.filepulse.scanner;

import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InMemoryStateBackingStore<V> implements StateBackingStore<V> {

    private final StateSnapshot<V> state;

    UpdateListener<V> listener;

    private Map<String, V> states = new HashMap<>();

    public InMemoryStateBackingStore(final StateSnapshot<V> state) {
        this.state = state;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public StateSnapshot<V> snapshot() {
        return state;
    }

    @Override
    public boolean contains(String name) {
        return states.containsKey(name);
    }

    @Override
    public void putAsync(String name, V state) {
        states.put(name, state);
        if (listener != null) {
            listener.onStateUpdate(name, state);
        }
    }

    @Override
    public void put(String name, V state) {
        states.put(name, state);
        if (listener != null) {
            listener.onStateUpdate(name, state);
        }
    }

    @Override
    public void remove(String name) {
        states.remove(name);
        if (listener != null) {
            listener.onStateRemove(name);
        }
    }

    @Override
    public void removeAsync(String name) {
        states.remove(name);
        if (listener != null) {
            listener.onStateRemove(name);
        }
    }

    @Override
    public void refresh(long timeout, TimeUnit unit) throws TimeoutException {

    }

    @Override
    public void setUpdateListener(final UpdateListener<V> listener) {
        this.listener = listener;
    }
}