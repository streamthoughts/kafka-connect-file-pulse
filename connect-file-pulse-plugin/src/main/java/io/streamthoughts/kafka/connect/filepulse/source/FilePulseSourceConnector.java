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

import io.streamthoughts.kafka.connect.filepulse.Version;
import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.config.ConnectorConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.CompositeFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultFileSystemMonitor;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemMonitor;
import io.streamthoughts.kafka.connect.filepulse.state.FileObjectStateBackingStoreManager;
import io.streamthoughts.kafka.connect.filepulse.state.internal.OpaqueMemoryResource;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC;
import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG;

/**
 * The FilePulseSourceConnector.
 */
public class FilePulseSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseSourceConnector.class);

    private static final long DEFAULT_MAX_TIMEOUT = 5000;

    private static final String CONNECT_NAME_CONFIG = "name";

    private Map<String, String> configProperties;

    private final AtomicInteger taskConfigsGeneration = new AtomicInteger(0);

    private FileSystemMonitorThread fsMonitorThread;

    private ConnectorConfig connectorConfig;

    private FileSystemMonitor scanner;

    private String connectorGroupName;

    private OpaqueMemoryResource<StateBackingStore<FileObject>> sharedStore;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * {@inheritDoc}
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(final Map<String, String> props) {
        connectorGroupName = props.get(CONNECT_NAME_CONFIG);
        LOG.info("Starting FilePulse source connector: {}", connectorGroupName);
        try {
            configProperties = new HashMap<>(props);
            configProperties.put(TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, connectorGroupName);
            configProperties.put(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC, "false");
            connectorConfig = new ConnectorConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("Failed to initialize FilePulseSourceConnector due to configuration error", e);
        }

        try {

            initSharedStateBackingStore(connectorConfig, connectorGroupName);

            final FileSystemListing<?> directoryScanner = this.connectorConfig.fileSystemListing();
            directoryScanner.setFilter(new CompositeFileListFilter(connectorConfig.fileSystemListingFilter()));

            final FileCleanupPolicy cleaner = connectorConfig.cleanupPolicy();
            final SourceOffsetPolicy offsetPolicy = connectorConfig.getSourceOffsetPolicy();

            scanner = new DefaultFileSystemMonitor(
                    connectorConfig.allowTasksReconfigurationAfterTimeoutMs(),
                    directoryScanner,
                    cleaner,
                    offsetPolicy,
                    sharedStore.getResource()
            );
            fsMonitorThread = new FileSystemMonitorThread(context, scanner, connectorConfig.scanInternalMs());
            fsMonitorThread.setUncaughtExceptionHandler((t, e) -> {
                LOG.info("Uncaught error from file system monitoring thread [{}]", t.getName(), e);
                throw new ConnectException(e);
            });
            fsMonitorThread.start();
            LOG.info("Started FilePulse source connector: {}", connectorGroupName);
        } catch (Exception e) {
            closeResources();
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<? extends Task> taskClass() {
        return FilePulseSourceTask.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOG.info("Creating new tasks configurations (maxTasks={})", maxTasks);
        List<List<String>> groupFiles = scanner
                .partitionFilesAndGet(maxTasks, connectorConfig.getMaxScheduledFiles())
                .stream()
                .map(l -> l.stream().map(URI::toString).collect(Collectors.toList()))
                .collect(Collectors.toList());

        final long taskConfigsGen = taskConfigsGeneration.getAndIncrement();
        final List<Map<String, String>> taskConfigs = new ArrayList<>(groupFiles.size());

        if (groupFiles.isEmpty()) {
            if (taskConfigsGen > 0) {
                LOG.info("No object file was found - skip task reconfiguration.");
                return taskConfigs;
            }
            LOG.info("No object file was found - resetting all tasks with an empty config.");
            IntStream.range(0, maxTasks).forEachOrdered(i -> taskConfigs.add(createTaskConfig("")));
        } else {
            groupFiles
                    .stream()
                    .map(group -> createTaskConfig(String.join(",", group)))
                    .forEach(taskConfigs::add);
        }

        IntStream.range(0, groupFiles.size()).forEachOrdered(i -> LOG.info(
                "Created config for task_id={} with '{}' object files (task_config_gen={}).",
                i,
                groupFiles.get(i).size(),
                taskConfigsGen)
        );
        return taskConfigs;
    }

    private Map<String, String> createTaskConfig(final String URIs) {
        final Map<String, String> taskProps = new HashMap<>(configProperties);
        taskProps.put(DefaultFileURIProvider.Config.FILE_OBJECT_URIS_CONFIG, URIs);
        return taskProps;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOG.info("Stopping FilePulse source connector");
        closeResources();
        LOG.info("Stopped FilePulse source connector");
    }

    private void closeResources() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("Closing resources for FilePulse source connector");
            try {
                if (fsMonitorThread != null) {
                    fsMonitorThread.shutdown(DEFAULT_MAX_TIMEOUT);
                    fsMonitorThread.join(DEFAULT_MAX_TIMEOUT);
                }
            } catch (InterruptedException e) {
                LOG.warn("Error while closing file-system monitoring thread: {}", e.getMessage());
            } finally {
                closeSharedStateBackingStore();
            }
            LOG.info("Closed resources for FilePulse source connector");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef config() {
        return ConnectorConfig.getConf();
    }

    private void initSharedStateBackingStore(final ConnectorConfig config, final String connectorGroupName) {
        try {
            sharedStore = FileObjectStateBackingStoreManager.INSTANCE
                    .getOrCreateSharedStore(
                            connectorGroupName,
                            config::getStateBackingStore,
                            new Object()
                    );
        } catch (Exception exception) {
            throw new ConnectException(
                    "Failed to create shared StateBackingStore for group '" + connectorGroupName + "'.",
                    exception
            );
        }
    }

    private void closeSharedStateBackingStore() {
        try {
            if (sharedStore != null) {
                sharedStore.close();
            }
        } catch (Exception exception) {
            LOG.error("Failed to shared StateBackingStore '{}'", connectorGroupName);
        }
    }
}