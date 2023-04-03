/*
 * Copyright 2019-2020 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code CompositeFileListFilter} can be used to combine multiple {@link FileListFilter} instances.
 */
public class CompositeFileListFilter implements FileListFilter, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeFileListFilter.class);

    private final Collection<FileListFilter> filters;

    /**
     * Creates a new {@link CompositeFileListFilter} instance.
     *
     * @param filters the list of filters to compose.
     */
    public CompositeFileListFilter(final Collection<FileListFilter> filters) {
        this.filters = new ArrayList<>(filters);
    }

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
    public Collection<FileObjectMeta> filterFiles(final Collection<FileObjectMeta> files) {
        LOG.debug("Starting to apply filters on source files '{}' files", files.size());
        long started = Time.SYSTEM.milliseconds();
        Collection<FileObjectMeta> results = new HashSet<>(files);
        filters.forEach(f -> {
            LOG.debug("Apply filter {}", f.getClass().getSimpleName());
            Collection<FileObjectMeta> currentFiltered = f.filterFiles(files);
            results.retainAll(currentFiltered);
        });
        LOG.debug("Finished to filter files - execution took {}ms", Time.SYSTEM.milliseconds() - started);
        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        for (FileListFilter f : filters) {
            if (f instanceof Closeable) {
                ((Closeable) f).close();
            }
        }
    }
}