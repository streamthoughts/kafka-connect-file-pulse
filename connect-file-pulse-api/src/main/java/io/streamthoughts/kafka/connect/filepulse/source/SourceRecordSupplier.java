/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Map;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Default interface to build connect output {@link SourceRecord}..
 */
public interface SourceRecordSupplier {

    /**
     * Returns a {@link SourceRecord} instance.
     *
     * @param sourcePartition   the source partition.
     * @param sourceOffset      the source offset.
     * @param metadata          the {@link LocalFileObjectMeta} to be used.
     * @param defaultTopic      the default topic to be used.
     * @param defaultPartition  the default partition to be used.
     * @param headers           the {@link ConnectHeaders} to be used.
     *
     * @return                  the new {@link SourceRecord} instance.
     */
    SourceRecord get(final Map<String, ?> sourcePartition,
                     final Map<String, ?> sourceOffset,
                     final FileObjectMeta metadata,
                     final String defaultTopic,
                     final Integer defaultPartition,
                     final ConnectHeaders headers);
}
