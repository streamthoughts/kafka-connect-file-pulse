/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;

/**
 * Policy for deleting completed files.
 */
public class DeleteCleanupPolicy implements FileCleanupPolicy {

    private Storage storage;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onSuccess(final FileObject source) {
        return storage.delete(source.metadata().uri());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final FileObject source) {
        return storage.delete(source.metadata().uri());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {
        this.storage = storage;
    }
}