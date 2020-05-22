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
package io.streamthoughts.kafka.connect.filepulse.scanner.local;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class LocalFSDirectoryWalkerTest {

    private static final String DEFAULT_ENTRY_FILE_NAME = "file-entry-0.txt";
    private static final String DEFAULT_ARCHIVE_NAME    = "archive";
    private static final String TEST_SCAN_DIRECTORY = "test-scan";

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private File inputDirectory;

    private FSDirectoryWalker scanner;

    @Before
    public void setUp() throws IOException {
        inputDirectory = folder.newFolder(TEST_SCAN_DIRECTORY);
        scanner = new LocalFSDirectoryWalker(Collections.emptyList());
    }

    @Test
    public void shouldExtractXZGipCompressedFilesPathWhileScanningGivenRecursiveScanDisable() throws IOException {
        File archiveFile = new File(inputDirectory, DEFAULT_ARCHIVE_NAME + ".zip");

        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archiveFile))) {
            zos.putNextEntry(new ZipEntry(DEFAULT_ENTRY_FILE_NAME));
            byte[] data = "dummy".getBytes();
            zos.write(data, 0, data.length);
            zos.closeEntry();
        }

        scanner.configure(Collections.singletonMap(LocalFSDirectoryWalkerConfig.FS_RECURSIVE_SCAN_ENABLE_CONFIG, false));
        final Collection<File> scanned = scanner.listFiles(inputDirectory);
        Assert.assertEquals(1, scanned.size());
        String expected = String.join(File.separator, Arrays.asList(inputDirectory.getCanonicalPath(), DEFAULT_ARCHIVE_NAME, DEFAULT_ENTRY_FILE_NAME));
        Assert.assertEquals(expected, scanned.iterator().next().getCanonicalPath());
    }

    @Test
    public void shouldListFilesGivenRecursiveScanEnable() throws IOException {
        folder.newFolder(TEST_SCAN_DIRECTORY , "sub-directory");
        final File file1 = folder.newFile(TEST_SCAN_DIRECTORY + "/test-file1.txt");
        final File file2 = folder.newFile(TEST_SCAN_DIRECTORY + "/sub-directory/test-file2.txt");

        scanner.configure(Collections.singletonMap(LocalFSDirectoryWalkerConfig.FS_RECURSIVE_SCAN_ENABLE_CONFIG, true));

        final Collection<String> scanned = scanner
                .listFiles(inputDirectory)
                .stream()
                .map(this::toCanonicalPath)
                .collect(Collectors.toList());

        Assert.assertEquals(2, scanned.size());
        Assert.assertTrue(scanned.contains(file1.getCanonicalPath()));
        Assert.assertTrue(scanned.contains(file2.getCanonicalPath()));
    }

    @Test
    public void shouldListFilesGivenRecursiveScanDisable() throws IOException {
        folder.newFolder(TEST_SCAN_DIRECTORY , "sub-directory");
        final File file1 = folder.newFile(TEST_SCAN_DIRECTORY + "/test-file1.txt");
        folder.newFile(TEST_SCAN_DIRECTORY + "/sub-directory/test-file2.txt"); // will not be scanned

        scanner.configure(Collections.singletonMap(LocalFSDirectoryWalkerConfig.FS_RECURSIVE_SCAN_ENABLE_CONFIG, false));

        final Collection<String> scanned = scanner
                .listFiles(inputDirectory)
                .stream()
                .map(this::toCanonicalPath)
                .collect(Collectors.toList());

        Assert.assertEquals(1, scanned.size());
        Assert.assertTrue(scanned.contains(file1.getCanonicalPath()));
    }

    private String toCanonicalPath(final File f) {
        try {
            return f.getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}