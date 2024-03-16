/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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