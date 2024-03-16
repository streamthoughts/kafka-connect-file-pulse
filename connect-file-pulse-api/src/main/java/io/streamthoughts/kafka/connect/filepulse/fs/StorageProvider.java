/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

/**
 * The {@link StorageProvider} can be used for accessing underlying {@link Storage}.
 *
 * @param <T>   the storage type.
 */
public interface StorageProvider<T extends Storage> {

    /**
     * @return  the {@link Storage} attached to this reader.
     */
    T storage();

}
