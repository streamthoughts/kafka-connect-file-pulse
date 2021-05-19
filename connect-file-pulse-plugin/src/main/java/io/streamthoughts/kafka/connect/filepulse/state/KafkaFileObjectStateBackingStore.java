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

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.storage.KafkaStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class KafkaFileObjectStateBackingStore implements FileObjectStateBackingStore {

    private static final String KEY_PREFIX = "connect-file-pulse";

    private KafkaStateBackingStore<FileObject> store;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> props) {
        final KafkaFileObjectStateBackingStoreConfig config = new KafkaFileObjectStateBackingStoreConfig(props);
        this.store = new KafkaStateBackingStore<>(
                config.getTaskStorageTopic(),
                KEY_PREFIX,
                config.getTaskStorageName(),
                config.getTaskStorageConfigs(),
                new FileObjectSerde(),
                config.getTaskStorageConsumerEnabled()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        store.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        store.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStarted() {
        return store.isStarted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StateSnapshot<FileObject> snapshot() {
        return store.snapshot();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(final String name) {
        return store.contains(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAsync(final String name, final FileObject state) {
        store.putAsync(name, state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final String name, final FileObject state) {
        store.put(name, state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(final String name) {
        store.remove(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAsync(final String name) {
        store.removeAsync(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void refresh(final long timeout, final TimeUnit unit) throws TimeoutException {
        store.refresh(timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setUpdateListener(final UpdateListener<FileObject> listener) {
        store.setUpdateListener(listener);
    }
}