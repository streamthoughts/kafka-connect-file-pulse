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
package io.streamthoughts.kafka.connect.filepulse.scanner;

import io.streamthoughts.kafka.connect.filepulse.offset.ComposeOffsetStrategy;
import io.streamthoughts.kafka.connect.filepulse.offset.SimpleOffsetManager;
import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;
import io.streamthoughts.kafka.connect.filepulse.source.SourceStatus;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.scanner.local.FSDirectoryWalker;
import io.streamthoughts.kafka.connect.filepulse.scanner.local.FileListFilter;
import io.streamthoughts.kafka.connect.filepulse.state.FileStateBackingStore;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class LocalFileSystemScannerTest {

    private static final SimpleOffsetManager OFFSET_MANAGER = new SimpleOffsetManager(new ComposeOffsetStrategy("name"));

    private static TemporaryFolder INPUT_DIRECTORY = new TemporaryFolder();

    private static TemporaryFileInput INPUT_FILES = new TemporaryFileInput(INPUT_DIRECTORY)
                    .withInputFiles("file1.text", "file2.txt", "file3.txt", "file4.txt");

    @Rule
    public TestRule chain = RuleChain.outerRule(INPUT_DIRECTORY).around(INPUT_FILES);

    @Test
    public void shouldReturnAllScannedFilesWhenNoStateExist() {
        StateSnapshot<SourceFile> state = new StateSnapshot<>(0, Collections.emptyMap());
        FileStateBackingStore store = Mockito.mock(FileStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);

        final MockTimesDirectoryScanner ds = new MockTimesDirectoryScanner(sources);
        LocalFileSystemScanner scanner = newFsMonitorThread(new MockFileCleaner(true), ds, store);

        scanner.scan(new MockConnectorContext());

        List<List<String>> result = scanner.partitionFilesAndGet(1);
        assertNotNull(result);
        assertEquals(1, result.size());
        List<String> expected = sources.stream().map(File::getAbsolutePath).sorted().collect(Collectors.toList());
        List<String> actual = result.get(0);
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void shouldCleanRemainingCompletedFilesWhileStarting() {
        Map<String, SourceFile> states = new HashMap<String, SourceFile>() {
            {
                put("???", INPUT_FILES.stateFor(0, SourceStatus.COMPLETED));
            }
        };
        StateSnapshot<SourceFile> state = new StateSnapshot<>(0, states);
        FileStateBackingStore store = Mockito.mock(FileStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesDirectoryScanner ds = new MockTimesDirectoryScanner(sources);
        final MockFileCleaner cleaner = new MockFileCleaner(true);
        LocalFileSystemScanner scanner = newFsMonitorThread(cleaner, ds, store);

        scanner.scan(new MockConnectorContext());

        assertEquals(1, cleaner.getSucceed().size());
        assertEquals(sources.get(0), cleaner.getSucceed().get(0));
    }

    @Test
    public void shouldFilterScannedFilesWhenStateExist() {

        final SourceFile completed = INPUT_FILES.stateFor(0, SourceStatus.COMPLETED);
        Map<String, SourceFile> states = new HashMap<String, SourceFile>() {
            {
                put(OFFSET_MANAGER.toPartitionJson(completed.metadata()), completed);
            }
        };
        StateSnapshot<SourceFile> state = new StateSnapshot<>(0, states);
        FileStateBackingStore store = Mockito.mock(FileStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesDirectoryScanner ds = new MockTimesDirectoryScanner(sources);
        LocalFileSystemScanner scanner = newFsMonitorThread(new MockFileCleaner(true), ds, store);

        scanner.scan(new MockConnectorContext());

        List<List<String>> groupedFiles = scanner.partitionFilesAndGet(1);
        assertNotNull(groupedFiles);
        assertEquals(1, groupedFiles.size());

        List<String> files = groupedFiles.get(0);
        assertEquals(1, files.size());
        assertEquals(sources.get(1).getAbsolutePath(), files.get(0));
    }

    @Test
    public void shouldScheduledFileWithExistingReadingState() {
        final SourceFile completed = INPUT_FILES.stateFor(0, SourceStatus.COMPLETED);
        final SourceFile reading = INPUT_FILES.stateFor(1, SourceStatus.READING);
        Map<String, SourceFile> states = new HashMap<String, SourceFile>() {
            {
                put(OFFSET_MANAGER.toPartitionJson(completed.metadata()), completed);
                put(OFFSET_MANAGER.toPartitionJson(reading.metadata()), reading);
            }
        };
        StateSnapshot<SourceFile> state = new StateSnapshot<>(0, states);
        FileStateBackingStore store = Mockito.mock(FileStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesDirectoryScanner ds = new MockTimesDirectoryScanner(sources);
        LocalFileSystemScanner scanner = newFsMonitorThread(new MockFileCleaner(true), ds, store);

        scanner.scan(new MockConnectorContext());

        List<List<String>> groupedFiles = scanner.partitionFilesAndGet(1);
        assertNotNull(groupedFiles);
        assertEquals(1, groupedFiles.size());

        List<String> files = groupedFiles.get(0);
        assertEquals(1, files.size());
        assertEquals(reading.metadata().absolutePath(), files.get(0));
    }

    @Test
    public void shouldNotScannedDirectoryWhileProcessingFiles() {
        final StateSnapshot<SourceFile> state = new StateSnapshot<>(0, Collections.emptyMap());
        FileStateBackingStore store = Mockito.mock(FileStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesDirectoryScanner ds = new MockTimesDirectoryScanner(sources);
        final LocalFileSystemScanner scanner = newFsMonitorThread(new MockFileCleaner(true), ds, store);

        MockConnectorContext context = new MockConnectorContext();
        scanner.scan(context);
        scanner.scan(context);

        assertEquals(1, ds.times());
    }

    @Test
    public void shouldCleanUpFilesAfterReceivingCompletedState() {

        final StateSnapshot<SourceFile> state = new StateSnapshot<>(0, Collections.emptyMap());
        final InMemoryStateBackingStore<SourceFile> store = new InMemoryStateBackingStore<>(state);
        final MockFileCleaner cleaner = new MockFileCleaner(true);
        final MockTimesDirectoryScanner ds = new MockTimesDirectoryScanner();
        final LocalFileSystemScanner scanner = newFsMonitorThread(cleaner, ds, store);

        ds.put(INPUT_FILES.getInputPathsFor(0, 1));
        scanner.scan(new MockConnectorContext());

        assertEquals(0, cleaner.getSucceed().size());
        assertEquals(0, cleaner.getFailed().size());

        final SourceFile completed = INPUT_FILES.stateFor(0, SourceStatus.COMPLETED);
        store.listener.onStateUpdate(OFFSET_MANAGER.toPartitionJson(completed.metadata()), completed);

        final SourceFile failed = INPUT_FILES.stateFor(1, SourceStatus.FAILED);
        store.listener.onStateUpdate(OFFSET_MANAGER.toPartitionJson(failed.metadata()), failed);

        ds.put(INPUT_FILES.getInputPathsFor(0, 1, 2, 3));
        scanner.scan(new MockConnectorContext());

        assertEquals(2, ds.times());

        assertEquals(1, cleaner.getSucceed().size());
        assertEquals(INPUT_FILES.metadataFor(0).absolutePath(), cleaner.getSucceed().get(0).getAbsolutePath());

        assertEquals(1, cleaner.getFailed().size());
        assertEquals(INPUT_FILES.metadataFor(1).absolutePath(), cleaner.getFailed().get(0).getAbsolutePath());
    }

    private LocalFileSystemScanner newFsMonitorThread(final MockFileCleaner cleaner,
                                                      final FSDirectoryWalker scanner,
                                                      final StateBackingStore<SourceFile> store) {
        return new LocalFileSystemScanner(
                INPUT_FILES.inputDirectory().getAbsolutePath(),
                scanner,
                cleaner,
                OFFSET_MANAGER,
                store
        );
    }

    private static class NoOpDirectoryScanner implements FSDirectoryWalker {

        @Override
        public void configure(Map<String, ?> configs) {

        }

        @Override
        public Collection<File> listFiles(File dir) throws IllegalArgumentException {
            return Collections.emptyList();
        }

        @Override
        public void setFilter(FileListFilter filter) {
        }
    }

    private static class MockTimesDirectoryScanner extends NoOpDirectoryScanner {

        private final List<Collection<File>> files = new ArrayList<>();
        private int times = 0;

        MockTimesDirectoryScanner() {
        }

        MockTimesDirectoryScanner(final List<File> files) {
            this.files.add(files);
        }

        @Override
        public Collection<File> listFiles(File dir) throws IllegalArgumentException {
            times++;
            return (files.isEmpty()) ? Collections.emptyList() : files.remove(0);
        }

        void put(Collection<File> files) {
            this.files.add(files);
        }

        int times() {
            return times;
        }
    }

    private static class MockConnectorContext implements ConnectorContext {

        @Override
        public void requestTaskReconfiguration() {

        }

        @Override
        public void raiseError(Exception e) {

        }
    }
}