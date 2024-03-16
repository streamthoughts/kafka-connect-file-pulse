/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import java.util.Map;

/**
 * Policy for cleaning individual completed source files.
 */
public interface FileCleanupPolicy extends
        GenericFileCleanupPolicy<FileObject, Boolean> {

    /**
     * {@inheritDoc}
     */
    @Override
    default void configure(final Map<String, ?> configs) { }

    /**
     * {@inheritDoc}
     */
    @Override
    default Boolean apply(final FileObject source) {
        final FileObjectStatus status = source.status();
        return isSuccess(status) ? onSuccess(source) : onFailure(source);
    }

    private boolean isSuccess(final FileObjectStatus status) {
        return status.equals(FileObjectStatus.COMPLETED) || status.equals(FileObjectStatus.COMMITTED);
    }

    boolean onSuccess(final FileObject source);

    boolean onFailure(final FileObject source);

}