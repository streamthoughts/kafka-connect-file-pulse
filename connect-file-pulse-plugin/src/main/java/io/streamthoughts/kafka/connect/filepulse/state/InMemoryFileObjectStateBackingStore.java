/*
 * Copyright 2021 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.annotation.VisibleForTesting;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-memory {@link StateBackingStore} implementation that uses an LRU cache based on HashMap.
 */
public class InMemoryFileObjectStateBackingStore implements FileObjectStateBackingStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryFileObjectStateBackingStore.class);

    private static final int DEFAULT_MAX_SIZE_CAPACITY = 10_000;

    private volatile Map<String, FileObject> objects;

    private StateBackingStore.UpdateListener<FileObject> listener;

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Creates a new {@link InMemoryFileObjectStateBackingStore} instance.
     */
    public InMemoryFileObjectStateBackingStore() { }

    @VisibleForTesting
    public InMemoryFileObjectStateBackingStore(final Map<String, FileObject> objects) {
        configure(Collections.emptyMap());
        this.objects.putAll(objects);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        FileObjectStateBackingStore.super.configure(configs);
        int cacheMaxCapacity = new Config(configs).getCacheMaxCapacity();
        this.objects = Collections.synchronizedMap(createLRUCache(cacheMaxCapacity, objectEntry -> {
            if (!objectEntry.getValue().status().isDone()) {
                LOG.warn(
                "Evicting a file-object state '{}' from in-memory state with a non terminal"
                + " status (i.e. 'CLEANED'). This may happen if you are processing more files than the"
                + " max-capacity of the InMemoryFileObjectStateBackingStore before committing offsets"
                + " for tasks successfully. Please consider increasing the value of"
                + " 'tasks.file.status.storage.cache.max.size.capacity' through"
                + " the connector's configuration.",
                objectEntry.getValue().metadata().stringURI()
                );
            }
        }));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        started.compareAndSet(false, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        started.compareAndSet(true, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStarted() {
        return started.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StateSnapshot<FileObject> snapshot() {
        return new StateSnapshot<>(-1, Collections.unmodifiableMap(objects));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(final String name) {
        return objects.containsKey(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAsync(final String name, final FileObject object) {
        put(name, object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final String name, final FileObject object) {
        LOG.debug("Put object in store with key={}, object={}", name, object);
        objects.put(name, object);
        if (listener != null) {
            listener.onStateUpdate(name, object);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(final String name) {
        LOG.debug("Remove object in store with key={}", name);
        objects.remove(name);
        if (listener != null) {
            listener.onStateRemove(name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAsync(final String name) {
        remove(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void refresh(final long timeout, final TimeUnit unit) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setUpdateListener(final UpdateListener<FileObject> listener) {
        this.listener = listener;
    }

    @VisibleForTesting
    public UpdateListener<FileObject> getListener() {
        return listener;
    }

    private static <K, V> Map<K, V> createLRUCache(final int maxCacheSize,
                                                   final Consumer<Map.Entry<K, V>> callbackOnRemoveEldest) {
        return new LinkedHashMap<>(maxCacheSize + 1, 1.01f, true) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
                boolean remove = size() > maxCacheSize;
                if (remove) {
                    callbackOnRemoveEldest.accept(eldest);
                }
                return remove;
            }
        };
    }

    private static final class Config extends AbstractConfig {

        private static final String GROUP = "InMemoryFileObjectStateBackingStore";

        public static final String TASKS_FILE_STATUS_STORAGE_CACHE_MAX_SIZE_CAPACITY_CONFIG
                = "tasks.file.status.storage.cache.max.size.capacity";
        private static final String TASKS_FILE_STATUS_STORAGE_CACHE_MAX_SIZE_CAPACITY_DOC
                = "The max size capacity of the LRU in-memory cache (default: 10_000).";

        /**
         * Creates a new {@link Config} instance.
         *
         * @param originals the configuration properties.
         */
        public Config(final Map<?, ?> originals) {
            super(configDef(), originals, false);
        }

        public int getCacheMaxCapacity() {
            return this.getInt(TASKS_FILE_STATUS_STORAGE_CACHE_MAX_SIZE_CAPACITY_CONFIG);
        }

        private static ConfigDef configDef() {
            int groupCounter = 0;
            return new ConfigDef()
                    .define(
                            TASKS_FILE_STATUS_STORAGE_CACHE_MAX_SIZE_CAPACITY_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_MAX_SIZE_CAPACITY,
                            ConfigDef.Importance.LOW,
                            TASKS_FILE_STATUS_STORAGE_CACHE_MAX_SIZE_CAPACITY_DOC,
                            GROUP,
                            groupCounter++,
                            ConfigDef.Width.NONE,
                            TASKS_FILE_STATUS_STORAGE_CACHE_MAX_SIZE_CAPACITY_CONFIG
                    );
        }
    }
}
