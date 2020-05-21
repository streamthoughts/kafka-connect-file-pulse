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
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;
import io.streamthoughts.kafka.connect.filepulse.source.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.source.SourceStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeleteCleanupPolicyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File file;

    private SourceFile source;

    @Before
    public void setUp() throws IOException {
        file = folder.newFile("file");

        source = new SourceFile(
                SourceMetadata.fromFile(file),
                SourceOffset.empty(),
                SourceStatus.COMPLETED,
                Collections.emptyMap());
    }

    @Test
    public void shouldDeleteSourceFileGivenCompletedSource() {
        DeleteCleanupPolicy policy = new DeleteCleanupPolicy();
        policy.configure(Collections.emptyMap());

        Boolean result = policy.apply(source.withStatus(SourceStatus.COMPLETED));
        assertTrue(result);
        assertFalse(Files.exists(file.toPath()));
    }

    @Test
    public void shouldDeleteSourceFileGivenFailedSource() {
        DeleteCleanupPolicy policy = new DeleteCleanupPolicy();
        policy.configure(Collections.emptyMap());

        Boolean result = policy.apply(source.withStatus(SourceStatus.FAILED));
        assertTrue(result);
        assertFalse(Files.exists(file.toPath()));
    }
}