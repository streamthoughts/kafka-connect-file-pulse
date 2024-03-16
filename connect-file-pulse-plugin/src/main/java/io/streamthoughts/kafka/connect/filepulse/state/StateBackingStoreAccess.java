/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.state;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.state.internal.OpaqueMemoryResource;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import java.util.function.Supplier;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code StateBackingStoreAccess} holds an access to a shared {@link StateBackingStore} instance.
 */
public final class StateBackingStoreAccess implements
        Supplier<OpaqueMemoryResource<StateBackingStore<FileObject>>>,
        AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StateBackingStoreAccess.class);

    private final String name;
    private final OpaqueMemoryResource<StateBackingStore<FileObject>> sharedStore;

    public StateBackingStoreAccess(final String name,
                                   final Supplier<StateBackingStore<FileObject>> supplier,
                                   final boolean doStart) {
        this.name = name;
        this.sharedStore = initSharedStateBackingStore(name, supplier, doStart);
    }

    private OpaqueMemoryResource<StateBackingStore<FileObject>> initSharedStateBackingStore(
            final String name,
            final Supplier<StateBackingStore<FileObject>> supplier,
            final boolean doStart) {
        try {
            LOG.info("Retrieving access to shared backing store");
            return FileObjectStateBackingStoreManager.INSTANCE
                    .getOrCreateSharedStore(
                            name,
                            () -> {
                                final StateBackingStore<FileObject> store = supplier.get();
                                // Always invoke the start() method when store is created from Task
                                // because this means the connector is running on a remote worker.
                                if (doStart) {
                                    store.start();
                                }
                                return store;
                            },
                            new Object()
                    );
        } catch (Exception exception) {
            throw new ConnectException(
                    "Failed to create shared StateBackingStore for group '" + name + "'.",
                    exception
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            if (sharedStore != null) {
                sharedStore.close();
            }
        } catch (Exception exception) {
            LOG.error("Failed to shared StateBackingStore '{}'", name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OpaqueMemoryResource<StateBackingStore<FileObject>> get() {
        return sharedStore;
    }
}
