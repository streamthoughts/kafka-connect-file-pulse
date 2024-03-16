/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.MockFileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.fs.clean.LogCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.LocalRowFileInputReader;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SourceConnectorConfigTest {

    private static final Map<String, Object> DEFAULT_CONFIG = Map.of(
            SourceConnectorConfig.FS_LISTING_CLASS_CONFIG, MockFileSystemListing.class,
            SourceConnectorConfig.TASKS_FILE_READER_CLASS_CONFIG, LocalRowFileInputReader.class.getName(),
            SourceConnectorConfig.FS_CLEANUP_POLICY_CLASS_CONFIG, LogCleanupPolicy.class,
            CommonSourceConfig.OUTPUT_TOPIC_CONFIG, "TOPIC_TEST"
    );

    @Test
    void should_return_default_config_for_default_read_timeout_ms() {
        // Given
        SourceConnectorConfig config = new SourceConnectorConfig(DEFAULT_CONFIG);
        // When
        Duration result = config.getStateDefaultReadTimeoutMs();
        // Then
        Assertions.assertEquals(SourceConnectorConfig.STATE_DEFAULT_READ_TIMEOUT_MS_DEFAULT, result.toMillis());
    }

    @Test
    void should_return_default_config_for_initial_read_timeout_ms() {
        // Given
        SourceConnectorConfig config = new SourceConnectorConfig(DEFAULT_CONFIG);
        // When
        Duration result = config.getStateInitialReadTimeoutMs();
        // Then
        Assertions.assertEquals(SourceConnectorConfig.STATE_INITIAL_READ_TIMEOUT_MS_DEFAULT, result.toMillis());
    }


}