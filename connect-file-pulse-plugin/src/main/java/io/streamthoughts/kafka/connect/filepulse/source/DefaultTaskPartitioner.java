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
