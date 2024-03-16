/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.fs.LocalFileStorage;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;

public abstract class BaseLocalFileInputReader
        extends AbstractFileInputReader
        implements StorageAwareFileInputReader<LocalFileStorage> {

    private final LocalFileStorage storage = new LocalFileStorage();

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalFileStorage storage() {
        return storage;
    }
}
