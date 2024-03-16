/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Policy for printing into log files completed files.
 */
public class LogCleanupPolicy implements FileCleanupPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(LogCleanupPolicy.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {

    }

    /**
     * {@inheritDoc}
     */
    public boolean onSuccess(final FileObject source) {
        LOG.info("Success : {}", source);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean onFailure(final FileObject source) {
        LOG.info("Failure : {}", source);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }

    /**
     * {@inheritDoc}
     */
    public void setStorage(final Storage storage) {

    }
}