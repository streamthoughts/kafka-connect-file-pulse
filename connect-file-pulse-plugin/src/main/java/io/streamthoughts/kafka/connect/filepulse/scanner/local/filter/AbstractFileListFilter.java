/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.scanner.local.filter;

import io.streamthoughts.kafka.connect.filepulse.scanner.local.FileListFilter;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class AbstractFileListFilter implements FileListFilter {

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> config) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Collection<File> filterFiles(final Collection<File> files) {
        List<File> accepted = new ArrayList<>();
        if (files != null) {
            accepted = files
                    .stream()
                    .filter(this::accept)
                    .collect(Collectors.toList());
        }
        return accepted;
    }

    protected abstract boolean accept(final File file);
}