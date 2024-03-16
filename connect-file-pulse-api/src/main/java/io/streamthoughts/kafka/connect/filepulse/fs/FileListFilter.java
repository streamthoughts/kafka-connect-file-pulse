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
 * Interface which is used to filter files scanned from a file-system.
 */
public interface FileListFilter extends Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    default void configure(final Map<String, ?> config) {

    }

    /**
     * Apply the filters on the specified list of files.
     *
     * @param files files to filter
     * @return      a new filtered list of files.
     */
    Collection<FileObjectMeta> filterFiles(final Collection<FileObjectMeta> files);
}
