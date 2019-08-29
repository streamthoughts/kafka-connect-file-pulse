/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.state;

import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class StateBackingStoreRegistryTest {

    private static final String TEST_STORE = "test-store";

    @Test
    public void test() {

        final StateBackingStoreRegistry registry = StateBackingStoreRegistry.instance();
        registry.register(TEST_STORE, MockStateBackingStore::new);

        StateBackingStore store = registry.get(TEST_STORE);
        registry.get(TEST_STORE);

        assertTrue(registry.has(TEST_STORE));
        registry.release(TEST_STORE);
        assertTrue(registry.has(TEST_STORE));
        assertFalse(((MockStateBackingStore)store).isStopped);

        registry.release(TEST_STORE);
        assertFalse(registry.has(TEST_STORE));
        assertTrue(((MockStateBackingStore)store).isStopped);
    }

    private static class MockStateBackingStore<T> implements StateBackingStore<T> {

        public boolean isStopped = false;

        @Override
        public void start() {

        }

        @Override
        public void stop() {
            isStopped = true;
        }

        @Override
        public StateSnapshot<T> snapshot() {
            return null;
        }

        @Override
        public boolean contains(String name) {
            return false;
        }

        @Override
        public void putAsync(String name, T state) {

        }

        @Override
        public void put(String name, T state) {

        }

        @Override
        public void remove(String name) {

        }

        @Override
        public void removeAsync(String name) {

        }

        @Override
        public void refresh(long timeout, TimeUnit unit) throws TimeoutException {

        }

        @Override
        public void setUpdateListener(UpdateListener listener) {

        }
    }

}