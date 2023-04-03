/*
 * Copyright 2021 StreamThoughts.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.streamthoughts.kafka.connect.filepulse.fs.LocalFileStorage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DeleteCleanupPolicyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File file;

    private FileObject source;

    @Before
    public void setUp() throws IOException {
        file = folder.newFile("file");

        source = new FileObject(
                new LocalFileObjectMeta(file),
                FileObjectOffset.empty(),
                FileObjectStatus.COMPLETED);
    }

    @Test
    public void should_delete_source_file_given_completed_source() {
        DeleteCleanupPolicy policy = new DeleteCleanupPolicy();
        policy.setStorage(new LocalFileStorage());
        policy.configure(Collections.emptyMap());

        Boolean result = policy.apply(source.withStatus(FileObjectStatus.COMPLETED));
        assertTrue(result);
        assertFalse(Files.exists(file.toPath()));
    }

    @Test
    public void should_delete_source_file_given_failed_source() {
        DeleteCleanupPolicy policy = new DeleteCleanupPolicy();
        policy.setStorage(new LocalFileStorage());
        policy.configure(Collections.emptyMap());

        Boolean result = policy.apply(source.withStatus(FileObjectStatus.FAILED));
        assertTrue(result);
        assertFalse(Files.exists(file.toPath()));
    }
}