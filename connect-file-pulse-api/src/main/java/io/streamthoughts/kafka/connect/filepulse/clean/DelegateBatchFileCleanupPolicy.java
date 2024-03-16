/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DelegateBatchFileCleanupPolicy implements BatchFileCleanupPolicy {

    private final FileCleanupPolicy delegate;

    /**
     * Creates a new {@link DelegateBatchFileCleanupPolicy} instance.
     *
     * @param delegate  the policy to be executed.
     */
    public DelegateBatchFileCleanupPolicy(final FileCleanupPolicy delegate) {
        Objects.requireNonNull(delegate, "delegate cannot be null");
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        delegate.configure(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileCleanupPolicyResultSet apply(final List<FileObject> sources) {
        FileCleanupPolicyResultSet rs = new FileCleanupPolicyResultSet();
        for (FileObject source : sources) {
            if (delegate.apply(source)) {
                rs.add(source, FileCleanupPolicyResult.SUCCEED);
            } else {
                rs.add(source, FileCleanupPolicyResult.FAILED);
            }
        }
        return rs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        delegate.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {
        delegate.setStorage(storage);
    }
}
