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
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import static org.junit.Assert.assertTrue;

import io.streamthoughts.kafka.connect.filepulse.fs.LocalFSDirectoryListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.LocalFileStorage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalMoveCleanupPolicyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File succeedDirectory;
    private File failedDirectory;
    private File file;

    private Map<String, String> config;

    private FileObject source;

    @Before
    public void setUp() throws IOException {

        succeedDirectory = folder.newFolder("succeed");
        failedDirectory = folder.newFolder("failed");

        file = folder.newFile("file");

         source = new FileObject(
                 new LocalFileObjectMeta(file),
                 FileObjectOffset.empty(),
                 FileObjectStatus.COMPLETED);

        config = new HashMap<>();

        config.put(LocalMoveCleanupPolicy.MoveFileCleanerConfig.CLEANER_OUTPUT_SUCCEED_PATH_CONFIG, succeedDirectory.getAbsolutePath());
        config.put(LocalMoveCleanupPolicy.MoveFileCleanerConfig.CLEANER_OUTPUT_FAILED_PATH_CONFIG, failedDirectory.getAbsolutePath());
        config.put(LocalFSDirectoryListingConfig.FS_LISTING_DIRECTORY_PATH, folder.getRoot().getAbsolutePath());
    }

    @Test
    public void should_move_file_given_completed_source() {
        final LocalMoveCleanupPolicy policy = new LocalMoveCleanupPolicy();
        policy.setStorage(new LocalFileStorage());
        policy.configure(config);

        Boolean result = policy.apply(source.withStatus(FileObjectStatus.COMPLETED));
        assertTrue(Files.exists(Paths.get(succeedDirectory.getAbsolutePath(), file.getName())));
        assertTrue(result);
    }

    @Test
    public void should_move_file_given_failed_source() {
        final LocalMoveCleanupPolicy policy = new LocalMoveCleanupPolicy();
        policy.setStorage(new LocalFileStorage());
        policy.configure(config);

        Boolean result = policy.apply(source.withStatus(FileObjectStatus.FAILED));
        assertTrue(Files.exists(Paths.get(failedDirectory.getAbsolutePath(), file.getName())));
        assertTrue(result);
    }

}