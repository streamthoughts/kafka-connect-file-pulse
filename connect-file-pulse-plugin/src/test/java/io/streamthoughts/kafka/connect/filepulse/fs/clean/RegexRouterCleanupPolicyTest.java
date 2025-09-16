/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.LocalFileStorage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RegexRouterCleanupPolicyTest {

    private File file;

    private FileObject source;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        final Path tempFile = Files.createFile(tempDir.resolve("file.txt"));

        file = tempFile.toFile();
        source = new FileObject(
                new LocalFileObjectMeta(file),
                FileObjectOffset.empty(),
                FileObjectStatus.COMPLETED
        );
    }

    @Test
    void should_not_rename_file_given_cleanup_with_default_config() throws Exception {
        try (FileCleanupPolicy policy = new RegexRouterCleanupPolicy()) {
            policy.setStorage(new LocalFileStorage());
            policy.configure(Collections.emptyMap());

            boolean result = policy.apply(source.withStatus(FileObjectStatus.COMPLETED));
            Assertions.assertTrue(result);
            Assertions.assertTrue(Files.exists(file.toPath()));
        }
    }

    @Test
    void should_rename_success_file_given_cleanup_with_custom_config() {
        try (RegexRouterCleanupPolicy policy = new RegexRouterCleanupPolicy()) {
            policy.setStorage(new LocalFileStorage());
            policy.configure(Map.of(
                    RegexRouterCleanupPolicy.SUCCESS_ROUTE_TOPIC_REGEX_CONFIG,"(.*).txt",
                    RegexRouterCleanupPolicy.SUCCESS_ROUTE_TOPIC_REPLACEMENT_CONFIG, "$1-DONE-SUCCESS.txt"
            ));

            URI targetURI = policy.routeOnSuccess(source.metadata().uri());
            Assertions.assertTrue(targetURI.toString().endsWith("-DONE-SUCCESS.txt"));
            boolean result = policy.apply(source.withStatus(FileObjectStatus.COMPLETED));
            Assertions.assertTrue(result);
            Assertions.assertFalse(Files.exists(file.toPath()));
        }
    }

    @Test
    void should_rename_failure_file_given_cleanup_with_custom_config() {
        try (RegexRouterCleanupPolicy policy = new RegexRouterCleanupPolicy()) {
            policy.setStorage(new LocalFileStorage());
            policy.configure(Map.of(
                    RegexRouterCleanupPolicy.FAILURE_ROUTE_TOPIC_REGEX_CONFIG,"(.*).txt",
                    RegexRouterCleanupPolicy.FAILURE_ROUTE_TOPIC_REPLACEMENT_CONFIG, "$1-DONE-FAILURE.txt"
            ));

            URI targetURI = policy.routeOnFailure(source.metadata().uri());
            Assertions.assertTrue(targetURI.toString().endsWith("-DONE-FAILURE.txt"));
            boolean result = policy.apply(source.withStatus(FileObjectStatus.FAILED));
            Assertions.assertTrue(result);
            Assertions.assertFalse(Files.exists(file.toPath()));
        }
    }
}