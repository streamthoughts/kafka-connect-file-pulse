/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * An object representing a single record value that was read from an input file.
 *
 * @param <T>   the record-value type.
 */
public interface FileRecord<T> {

    /**
     * Gets the record value.
     *
     * @return  the value.
     */
    T value();

    /**
     * Returns the offset this record.
     *
     * @return a {@link FileRecordOffset} instance.
     */
    FileRecordOffset offset();

    /**
     * Transforms this file record into a connect {@link SourceRecord} instance.
     *
     * @param sourcePartition       the source partition.
     * @param sourceOffset          the source offset.
     * @param metadata              the {@link LocalFileObjectMeta} to be used.
     * @param defaultTopic          the default topic to be used.
     * @param defaultPartition      the default partition to be used.
     * @param connectSchemaSupplier the connect Schema to be used which can be {@code null}.
     *
     * @return                  the new {@link SourceRecord} instance.
     */
    SourceRecord toSourceRecord(
            final Map<String, ?> sourcePartition,
            final Map<String, ?> sourceOffset,
            final FileObjectMeta metadata,
            final String defaultTopic,
            final Integer defaultPartition,
            final Function<String, Schema> connectSchemaSupplier,
            final ConnectSchemaMapperOptions options
           );


    class ConnectSchemaMapperOptions {

        private final boolean connectSchemaMergeEnabled;
        private final boolean keepSchemaLeadingUnderscore;

        private final Pattern connectSchemaConditionTopicPattern;

        public ConnectSchemaMapperOptions(final boolean connectSchemaMergeEnabled,
                                          final boolean keepSchemaLeadingUnderscore,
                                          final Pattern connectSchemaConditionTopicPattern) {
            this.connectSchemaMergeEnabled = connectSchemaMergeEnabled;
            this.keepSchemaLeadingUnderscore = keepSchemaLeadingUnderscore;
            this.connectSchemaConditionTopicPattern = connectSchemaConditionTopicPattern;
        }

        public boolean isConnectSchemaMergeEnabled() {
            return connectSchemaMergeEnabled;
        }

        public boolean isKeepSchemaLeadingUnderscore() {
            return keepSchemaLeadingUnderscore;
        }

        public Pattern getConnectSchemaConditionTopicPattern() {
            return connectSchemaConditionTopicPattern;
        }
    }
}
