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
package io.streamthoughts.kafka.connect.filepulse.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.state.InMemoryFileObjectStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.KafkaStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import io.streamthoughts.kafka.connect.filepulse.utils.MockFileCleaner;
import io.streamthoughts.kafka.connect.filepulse.utils.TemporaryFileInput;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

public class DefaultFileSystemMonitorTest {

    private static final SourceOffsetPolicy OFFSET_MANAGER = new SourceOffsetPolicy() {
        @Override
        public Optional<FileObjectOffset> getOffsetFor(SourceTaskContext context, FileObjectMeta source) {
            return Optional.empty();
        }

        @Override
        public Map<String, ?> toOffsetMap(FileObjectOffset offset) {
            return Collections.singletonMap("key", offset.position());
        }

        @Override
        public Map<String, Object> toPartitionMap(FileObjectMeta meta) {
            return Collections.singletonMap("key", meta.name());
        }
    };

    private static final TemporaryFolder INPUT_DIRECTORY = new TemporaryFolder();

    private static final TemporaryFileInput INPUT_FILES = new TemporaryFileInput(INPUT_DIRECTORY)
                    .withInputFiles("file1.text", "file2.txt", "file3.txt", "file4.txt");

    @Rule
    public TestRule chain = RuleChain.outerRule(INPUT_DIRECTORY).around(INPUT_FILES);

    private static final StateSnapshot<FileObject> EMPTY_STATE_SNAPSHOT = new StateSnapshot<>(
            0,
            Collections.emptyMap()
    );

    @Test
    public void should_not_invoke_listing_when_monitor_is_not_fully_started() {
        KafkaStateBackingStore store = Mockito.mock(KafkaStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(EMPTY_STATE_SNAPSHOT);

        final MockTimesFileSystemListing ds = new MockTimesFileSystemListing(Collections.emptyList());
        DefaultFileSystemMonitor monitor = newFileSystemMonitor(new MockFileCleaner(true), ds, store);

        monitor.invoke(new MockConnectorContext());
        assertEquals(0, ds.times());

        monitor.listFilesToSchedule(0); // make a first call to mark the monitor as running.

        monitor.invoke(new MockConnectorContext());
        assertEquals(1, ds.times());
    }

    @Test
    public void should_return_all_scanned_files_when_no_state_exist() {
        KafkaStateBackingStore store = Mockito.mock(KafkaStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(EMPTY_STATE_SNAPSHOT);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);

        final MockTimesFileSystemListing ds = new MockTimesFileSystemListing(sources);
        DefaultFileSystemMonitor monitor = newFileSystemMonitor(new MockFileCleaner(true), ds, store);

        monitor.listFilesToSchedule(); // make a first call to mark the monitor as running.
        monitor.invoke(new MockConnectorContext());

        List<FileObjectMeta> result = monitor.listFilesToSchedule();
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(
            sources.stream().map(File::toURI).sorted().collect(Collectors.toList()),
            result.stream().map(FileObjectMeta::uri).sorted().collect(Collectors.toList()));
    }

    @Test
    public void should_clean_remaining_completed_files_while_starting() {
        Map<String, FileObject> states = new HashMap<>();
        states.put("???", INPUT_FILES.stateFor(0, FileObjectStatus.COMPLETED));

        StateSnapshot<FileObject> state = new StateSnapshot<>(0, states);
        KafkaStateBackingStore store = Mockito.mock(KafkaStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesFileSystemListing ds = new MockTimesFileSystemListing(sources);
        final MockFileCleaner cleaner = new MockFileCleaner(true);
        DefaultFileSystemMonitor monitor = newFileSystemMonitor(cleaner, ds, store);

        monitor.listFilesToSchedule(0); // make a first call to mark the monitor as running.
        monitor.invoke(new MockConnectorContext());

        Assert.assertEquals(1, cleaner.getSucceed().size());
        Assert.assertEquals(sources.get(0).toURI(), cleaner.getSucceed().get(0).uri());
    }

    @Test
    public void should_filter_scanned_files_when_state_exist() {

        final FileObject completed = INPUT_FILES.stateFor(0, FileObjectStatus.COMPLETED);
        Map<String, FileObject> states = new HashMap<>();
        states.put(OFFSET_MANAGER.toPartitionJson(completed.metadata()), completed);

        StateSnapshot<FileObject> state = new StateSnapshot<>(0, states);
        KafkaStateBackingStore store = Mockito.mock(KafkaStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesFileSystemListing ds = new MockTimesFileSystemListing(sources);
        DefaultFileSystemMonitor monitor = newFileSystemMonitor(new MockFileCleaner(true), ds, store);

        monitor.listFilesToSchedule(); // make a first call to mark the monitor as running.

        monitor.invoke(new MockConnectorContext());

        List<FileObjectMeta> groupedFiles = monitor.listFilesToSchedule();
        assertNotNull(groupedFiles);
        assertEquals(1, groupedFiles.size());
        assertEquals(sources.get(1).toURI(), groupedFiles.get(0).uri());
    }

    @Test
    public void should_scheduled_file_with_existing_reading_state() {
        final FileObject completed = INPUT_FILES.stateFor(0, FileObjectStatus.COMPLETED);
        final FileObject reading = INPUT_FILES.stateFor(1, FileObjectStatus.READING);
        Map<String, FileObject> states = new HashMap<>();
        states.put(OFFSET_MANAGER.toPartitionJson(completed.metadata()), completed);
        states.put(OFFSET_MANAGER.toPartitionJson(reading.metadata()), reading);

        StateSnapshot<FileObject> state = new StateSnapshot<>(0, states);
        KafkaStateBackingStore store = Mockito.mock(KafkaStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(state);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesFileSystemListing ds = new MockTimesFileSystemListing(sources);
        DefaultFileSystemMonitor monitor = newFileSystemMonitor(new MockFileCleaner(true), ds, store);

        monitor.listFilesToSchedule(0); // make a first call to mark the monitor as running.

        monitor.invoke(new MockConnectorContext());

        List<FileObjectMeta> groupedFiles = monitor.listFilesToSchedule();
        assertNotNull(groupedFiles);
        assertEquals(1, groupedFiles.size());
        assertEquals(sources.get(1).toURI(), groupedFiles.get(0).uri());
    }

    @Test
    public void should_not_scanned_directory_while_processing_files() {
        KafkaStateBackingStore store = Mockito.mock(KafkaStateBackingStore.class);
        Mockito.when(store.snapshot()).thenReturn(EMPTY_STATE_SNAPSHOT);

        final List<File> sources = INPUT_FILES.getInputPathsFor(0, 1);
        final MockTimesFileSystemListing ds = new MockTimesFileSystemListing(sources);
        final DefaultFileSystemMonitor monitor = newFileSystemMonitor(new MockFileCleaner(true), ds, store);

        MockConnectorContext context = new MockConnectorContext();
        monitor.invoke(context);

        monitor.listFilesToSchedule(); // Get files to simulate a scheduled.

        monitor.invoke(context);

        assertEquals(1, ds.times());
    }

    @Test
    public void should_cleanup_files_after_receiving_completed_state() {

        final InMemoryFileObjectStateBackingStore store = new InMemoryFileObjectStateBackingStore(Collections.emptyMap());
        final MockFileCleaner cleaner = new MockFileCleaner(true);
        final MockTimesFileSystemListing ds = new MockTimesFileSystemListing();
        final DefaultFileSystemMonitor monitor = newFileSystemMonitor(cleaner, ds, store);

        ds.put(INPUT_FILES.getInputPathsFor(0, 1));

        monitor.invoke(new MockConnectorContext());

        Assert.assertEquals(0, cleaner.getSucceed().size());
        Assert.assertEquals(0, cleaner.getFailed().size());

        final FileObject completed = INPUT_FILES.stateFor(0, FileObjectStatus.COMPLETED);
        store.getListener().onStateUpdate(OFFSET_MANAGER.toPartitionJson(completed.metadata()), completed);

        final FileObject failed = INPUT_FILES.stateFor(1, FileObjectStatus.FAILED);
        store.getListener().onStateUpdate(OFFSET_MANAGER.toPartitionJson(failed.metadata()), failed);

        ds.put(INPUT_FILES.getInputPathsFor(0, 1, 2, 3));

        monitor.invoke(new MockConnectorContext());

        Assert.assertEquals(1, cleaner.getSucceed().size());
        Assert.assertEquals(INPUT_FILES.metadataFor(0).uri(), cleaner.getSucceed().get(0).uri());

        Assert.assertEquals(1, cleaner.getFailed().size());
        Assert.assertEquals(INPUT_FILES.metadataFor(1).uri(), cleaner.getFailed().get(0).uri());
    }

    private DefaultFileSystemMonitor newFileSystemMonitor(final MockFileCleaner cleaner,
                                                          final FileSystemListing<Storage> fsListing,
                                                          final StateBackingStore<FileObject> store) {
        return new DefaultFileSystemMonitor(
                Long.MAX_VALUE,
                fsListing,
                cleaner,
                status -> List.of(FileObjectStatus.FAILED, FileObjectStatus.COMPLETED).contains(status),
                OFFSET_MANAGER,
                store,
                TaskFileOrder.BuiltIn.LAST_MODIFIED.get()
        );
    }

    private static class NoOpFileSystemListing implements FileSystemListing<Storage> {

        @Override
        public void configure(Map<String, ?> configs) {

        }

        @Override
        public Collection<FileObjectMeta> listObjects() throws IllegalArgumentException {
            return Collections.emptyList();
        }

        @Override
        public void setFilter(FileListFilter filter) {
        }

        @Override
        public Storage storage() {
            return null;
        }
    }

    private static class MockTimesFileSystemListing extends NoOpFileSystemListing {

        private final List<Collection<File>> files = new ArrayList<>();
        private int times = 0;

        MockTimesFileSystemListing() {
        }

        MockTimesFileSystemListing(final List<File> files) {
            this.files.add(files);
        }

        @Override
        public Collection<FileObjectMeta> listObjects() throws IllegalArgumentException {
            times++;
            return (files.isEmpty()) ?
                Collections.emptyList() :
                files.remove(0).stream().map(LocalFileObjectMeta::new).collect(Collectors.toList());
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