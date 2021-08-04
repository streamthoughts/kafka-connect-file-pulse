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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory {@link StateBackingStore} implementation.
 */
public class InMemoryFileObjectStateBackingStore implements FileObjectStateBackingStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryFileObjectStateBackingStore.class);

    private final ConcurrentHashMap<String, FileObject> objects = new ConcurrentHashMap<>();

    private StateBackingStore.UpdateListener<FileObject> listener;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public InMemoryFileObjectStateBackingStore() { }

    @VisibleForTesting
    public InMemoryFileObjectStateBackingStore(final Map<String, FileObject> objects) {
        this.objects.putAll(objects);
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
}
