/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A {@code PredicateFileListFilter} can be used to filter input files based on a given {@link Predicate}.
 */
public abstract class PredicateFileListFilter implements FileListFilter, Predicate<FileObjectMeta> {

    /**
     * {@inheritDoc}
     */
    @Override
    public final Collection<FileObjectMeta> filterFiles(final Collection<FileObjectMeta> files) {
        List<FileObjectMeta> accepted = new ArrayList<>();
        if (files != null) {
            accepted = files
            .stream()
            .filter(this)
            .collect(Collectors.toList());
        }
        return accepted;
    }
}