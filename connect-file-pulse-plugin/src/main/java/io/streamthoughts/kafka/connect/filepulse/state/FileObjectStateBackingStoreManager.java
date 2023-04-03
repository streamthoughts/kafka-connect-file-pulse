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
import io.streamthoughts.kafka.connect.filepulse.state.internal.OpaqueMemoryResource;
import io.streamthoughts.kafka.connect.filepulse.state.internal.ResourceDisposer;
import io.streamthoughts.kafka.connect.filepulse.state.internal.ResourceInitializer;
import io.streamthoughts.kafka.connect.filepulse.storage.KafkaStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code StateBackingStoreManager} is used to manage {@link StateBackingStore} shared
 * across Connector and Tasks instances.
 */
public final class FileObjectStateBackingStoreManager {

    public static final FileObjectStateBackingStoreManager INSTANCE = new FileObjectStateBackingStoreManager();

    private static final Logger LOG = LoggerFactory.getLogger(FileObjectStateBackingStoreManager.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final ConcurrentHashMap<String, LeasedResource<StateBackingStore<FileObject>>> leasedResources =
            new ConcurrentHashMap<>();

    /**
     * Gets the shared {@link FileObjectStateBackingStoreManager} and registers a lease.
     * If the object does not yet exist, it will be created through the given initializer.
     *
     * @param name          the store name.
     * @param initializer   the initializer function.
     * @param leaseHolder   the lease to register.
     *
     * @return              the shared {@link KafkaStateBackingStore<FileObject>}.
     */
    public OpaqueMemoryResource<StateBackingStore<FileObject>> getOrCreateSharedStore(
            final String name,
            final ResourceInitializer<StateBackingStore<FileObject>> initializer,
            final Object leaseHolder
    ) throws Exception {
        lock.lock();
        try {
            LeasedResource<StateBackingStore<FileObject>> leasedResource = this.leasedResources.get(name);
            if (leasedResource == null) {
                LOG.info("Initializing shared StateBackingStore '{}'", name);
                StateBackingStore<FileObject> resource = initializer.apply();
                leasedResource = new LeasedResource<>(resource);
                this.leasedResources.put(name, leasedResource);
            }

            leasedResource.addLeaseHolder(leaseHolder);
            ResourceDisposer<Exception> disposer = () -> release(name, leaseHolder);
            return new OpaqueMemoryResource<>(leasedResource.getResource(), disposer);
        } finally {
            lock.unlock();
        }
    }

    void release(final String name, final Object leaseHolder) throws Exception {
        lock.lock();
        try {
            LeasedResource<StateBackingStore<FileObject>> leasedResource = this.leasedResources.get(name);
            if (leasedResource == null) {
                return;
            }

            final String threadName = Thread.currentThread().getName();
            LOG.info("[{}] Releasing access on shared StateBackingStore '{}' instance.", name, threadName);
            if (leasedResource.removeLeaseHolder(leaseHolder)) {
                LOG.info("[{}] Closing shared StateBackingStore '{}'", name, threadName);
                leasedResource.close();
                leasedResources.remove(name);
            }
        } finally {
            lock.unlock();
        }
    }

    private final static class LeasedResource<T extends AutoCloseable> implements AutoCloseable {

        private final T resource;
        private final HashSet<Object> leaseHolders = new HashSet<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Creates a new {@link LeasedResource} instance.
         *
         * @param resource  the resource.
         */
        public LeasedResource(final T resource) {
            this.resource = Objects.requireNonNull(resource, "resource should not be null");
        }

        /**
         * Gets the resource.
         */
        public T getResource() {
            return resource;
        }

        /**
         * Adds a new lease to the handle resource.
         *
         * @param leaseHolder   the lease holder object.
         */
        void addLeaseHolder(final Object leaseHolder) {
            leaseHolders.add(leaseHolder);
        }

        /**
         * Removes the given lease holder.
         *
         * @param leaseHolder   the lease holder object.
         * @return              {@code true} is not use any more, and can be disposed.
         */
        boolean removeLeaseHolder(final Object leaseHolder) {
            leaseHolders.remove(leaseHolder);
            return leaseHolders.isEmpty();
        }

        /**
         * Disposes the resource handled.
         */
        @Override
        public void close() throws Exception {
            if (closed.compareAndSet(false, true)) {
                resource.close();
            }
        }
    }
}
