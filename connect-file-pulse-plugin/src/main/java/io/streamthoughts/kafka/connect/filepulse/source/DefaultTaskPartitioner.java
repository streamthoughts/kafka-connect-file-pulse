/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.util.ConnectorUtils;

public class DefaultTaskPartitioner implements TaskPartitioner {

    /**
     * {@inheritDoc}
     */
    @Override
    public List<List<URI>> partition(final Collection<FileObjectMeta> files, final int taskCount) {
        if (files.isEmpty()) {
            return Collections.emptyList();
        }

        final int numGroups = Math.min(files.size(), taskCount);
        return ConnectorUtils
                .groupPartitions(new ArrayList<>(files), numGroups)
                .stream()
                .map(this::toURIs)
                .collect(Collectors.toList());
    }

    private List<URI> toURIs(final List<FileObjectMeta> items) {
        return items.stream()
                .map(FileObjectMeta::uri)
                .collect(Collectors.toList());
    }
}
