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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StateBackingStoreRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(StateBackingStoreRegistry.class);

    private static final StateBackingStoreRegistry INSTANCE = new StateBackingStoreRegistry();

    public static StateBackingStoreRegistry instance() {
        return INSTANCE;
    }

    private final Map<String, Integer> refs;
    private final Map<String, StateBackingStore<SourceFile>> stores;

    /**
     * Creates a new {@link StateBackingStoreRegistry} instance.
     */
    private StateBackingStoreRegistry() {
        this.stores = new ConcurrentHashMap<>();
        this.refs = new ConcurrentHashMap<>();
    }

    public synchronized void register(final String name,
                                      final StateStoreProvider<SourceFile> provider) {
        Objects.requireNonNull(name, "name can't be null");
        Objects.requireNonNull(provider, "provider can't be null");

        LOG.info("Registering new store for name : {}", name);
        if (!has(name)) {
            stores.put(name, provider.get());
        } else {
            LOG.info("State store already registered for name : {}", name);
        }
    }

    public synchronized StateBackingStore<SourceFile> get(final String name) {
        Objects.requireNonNull(name, "name can't be null");
        checkIfExists(name);
        refs.compute(name, (k, v) ->  v == null ? 1 : v + 1 );
        final StateBackingStore<SourceFile> store = stores.get(name);
        LOG.info("Getting access on {} instance for group {}", store.getClass().getSimpleName(), name);
        return store;
    }

    public synchronized void release(final String name) {
        checkIfExists(name);
        StateBackingStore<?> store = stores.get(name);
        LOG.info("Releasing access on {} instance for group {}", store.getClass().getSimpleName(), name);
        final Integer ref = refs.compute(name, (k, v) -> v == null ? null : (v - 1 == 0) ? null : v -1);
        if (ref == null) {
            LOG.info("Stopping instance registered for group {}", store.getClass().getSimpleName(), name);
            store.stop();
            stores.remove(name);
        }
    }

    public boolean has(final String name) {
        return stores.containsKey(name);
    }

    private void checkIfExists(final String name) {
        if (!this.stores.containsKey(name)) {
            throw new IllegalArgumentException("No store registered for name : " + name);
        }
    }
}