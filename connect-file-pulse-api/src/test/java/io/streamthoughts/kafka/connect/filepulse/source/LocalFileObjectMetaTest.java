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
package io.streamthoughts.kafka.connect.filepulse.source;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalFileObjectMetaTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File file;

    @Before
    public void setUp() throws IOException {
        file = testFolder.newFile();
        try (BufferedWriter bw = Files.newBufferedWriter(file.toPath(), Charset.defaultCharset())) {
            bw.write("foo\n");
            bw.write("bar\n");
            bw.flush();
        }
    }

    @Test
    public void should_create_object_meta_given_file() {
        final LocalFileObjectMeta metadata = new LocalFileObjectMeta(file);

        String os = System.getProperty("os.name").toLowerCase();
        // only check if inode is non-empty on unix system.
        if (!os.startsWith("win") && !os.startsWith("mac")) {
            Assert.assertNotNull(metadata.inode());
        }
        Assert.assertNotEquals("-1", metadata.contentDigest().digest());
    }
}