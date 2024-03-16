/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * The {@code FileSystemListing} is used to list the object files that exists under a specific file-system.
 */
public interface FileSystemListing<T extends Storage> extends StorageProvider<T>, Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    void configure(final Map<String, ?> configs);

    /**
     * Lists all files existing into the specified input directory.
     *
     * @return      the list of all files found.
     */
    Collection<FileObjectMeta> listObjects();

    /**
     * Sets the filter to apply on each file during directory listing.
     * @param filter    the filter to apply.
     */
    void setFilter(final FileListFilter filter);



}
