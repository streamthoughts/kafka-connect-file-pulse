/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.fs.StorageProvider;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.net.URI;

public interface StorageAwareFileInputReader<T extends Storage> extends StorageProvider<T>, FileInputReader {

    /**
     * {@inheritDoc}
     */
    default FileObjectMeta getObjectMetadata(final URI objectURI) {
        return storage().getObjectMetadata(objectURI);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    default boolean canBeRead(final URI objectURI) {
        return storage().exists(objectURI);
    }
}
