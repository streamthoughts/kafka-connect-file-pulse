/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.utils;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MockFileCleaner implements FileCleanupPolicy {

    private final List<FileObjectMeta> succeed = new LinkedList<>();
    private final List<FileObjectMeta> failed = new LinkedList<>();

    private final boolean cleanUpReturn;

    public MockFileCleaner(boolean cleanUpReturn) {
        this.cleanUpReturn = cleanUpReturn;
    }

    public List<FileObjectMeta> getSucceed() {
        return succeed;
    }

    public List<FileObjectMeta> getFailed() {
        return failed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onSuccess(final FileObject source) {
        this.succeed.add(source.metadata());
        return cleanUpReturn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final FileObject source) {
        this.failed.add(source.metadata());
        return cleanUpReturn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {

    }
}