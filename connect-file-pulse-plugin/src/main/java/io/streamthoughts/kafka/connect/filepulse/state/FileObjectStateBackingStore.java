/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.state;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import java.util.Map;
import org.apache.kafka.common.Configurable;

public interface FileObjectStateBackingStore extends StateBackingStore<FileObject>, Configurable {

    /**
     * {@inheritDoc}
     */
    @Override
    default void configure(final Map<String, ?> configs) { }
}
