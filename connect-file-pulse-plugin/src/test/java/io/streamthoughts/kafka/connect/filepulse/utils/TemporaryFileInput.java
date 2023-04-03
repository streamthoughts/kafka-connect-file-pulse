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
package io.streamthoughts.kafka.connect.filepulse.utils;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TemporaryFileInput implements TestRule {

    private final TemporaryFolder folder;

    private File inputDirectory;

    private List<String> files;

    private List<FileObjectMeta> sources;

    public TemporaryFileInput(final TemporaryFolder folder) {
        this.folder = folder;
    }

    public TemporaryFileInput withInputFiles(final String... files) {
        this.files = Arrays.asList(files);
        return this;
    }

    @Override
    public Statement apply(final Statement statement, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                inputDirectory = folder.newFolder("tmp", "test-connect");
                createAllInputFiles(files);
                statement.evaluate();
            }
        };
    }

    public List<File> getInputPathsFor(final Integer... indexes) {
        List<Integer> l = Arrays.asList(indexes);
        List<File> paths = new ArrayList<>(l.size());
        for (Integer i : l) {
            paths.add(new File(metadataFor(i).uri()));
        }
        return paths;
    }

    public FileObjectMeta metadataFor(int index) {
        return sources.get(index);
    }

    public FileObject stateFor(int index, final FileObjectStatus status) {
        return new FileObject(
                metadataFor(index),
                new FileObjectOffset(0L, 0L, 0L),
                status
        );
    }

    public File inputDirectory() {
        return inputDirectory;
    }

    private void createAllInputFiles(final List<String> files) {
        this.sources = new ArrayList<>(files.size());
        for (String path : files) {
            File file = new File(inputDirectory, path);
            try {
                if (file.createNewFile()) {
                    sources.add(new LocalFileObjectMeta(file));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}