/*
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
package io.streamthoughts.kafka.connect.filepulse.scanner;

import io.streamthoughts.kafka.connect.filepulse.offset.SourceOffset;
import io.streamthoughts.kafka.connect.filepulse.offset.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.state.FileInputState;

import java.io.IOException;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TemporaryFileInput implements TestRule {

    private TemporaryFolder folder;

    private File inputDirectory;

    private List<String> files;

    private List<SourceMetadata> sources;

    TemporaryFileInput(final TemporaryFolder folder) {
        this.folder = folder;
    }

    TemporaryFileInput withInputFiles(final String... files) {
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

    List<File> getInputPathsFor(final Integer... indexes) {
        List<Integer> l = Arrays.asList(indexes);
        List<File> paths = new ArrayList<>(l.size());
        for (Integer i : l) {
            paths.add(new File(metadataFor(i).absolutePath()));
        }
        return paths;
    }

    SourceMetadata metadataFor(int index) {
        return sources.get(index);
    }

    FileInputState stateFor(int index, final FileInputState.Status status) {
        return new FileInputState(
                "",
                status,
                metadataFor(index),
                new SourceOffset(0L, 0L, 0L)
        );
    }

    File inputDirectory() {
        return inputDirectory;
    }

    private void createAllInputFiles(final List<String> files) {
        this.sources = new ArrayList<>(files.size());
        for (String path : files) {
            File file = new File(inputDirectory, path);
            try {
                if (file.createNewFile()) {
                    sources.add(SourceMetadata.fromFile(file));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}