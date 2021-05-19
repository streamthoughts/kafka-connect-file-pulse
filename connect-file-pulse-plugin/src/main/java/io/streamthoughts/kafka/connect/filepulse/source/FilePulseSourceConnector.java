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
import io.streamthoughts.kafka.connect.filepulse.config.TaskConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.CompositeFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultFileSystemMonitor;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC;
import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG;

/**
 * The FilePulseSourceConnector.
 */
public class FilePulseSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(FilePulseSourceConnector.class);

    private static final long MAX_TIMEOUT = 5000;

    private static final String CONNECT_NAME_CONFIG = "name";

    private Map<String, String> configProperties;

    private final AtomicInteger taskConfigsGeneration = new AtomicInteger(0);

    private FileSystemMonitorThread fsMonitorThread;

    private ConnectorConfig connectorConfig;

    private FileSystemMonitor scanner;

    private String connectorGroupName;

    private OpaqueMemoryResource<StateBackingStore<FileObject>> sharedStore;

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
        LOG.info("Configuring connector : {}", connectorGroupName);
        try {
            configProperties = new HashMap<>(props);
            configProperties.put(TASKS_FILE_STATUS_STORAGE_NAME_CONFIG, connectorGroupName);
            configProperties.put(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_DOC, "false");
            connectorConfig = new ConnectorConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("Failed to initialize FilePulseSourceConnector due to configuration error", e);
        }

        initSharedStateBackingStore(connectorConfig, connectorGroupName);

        final FileSystemListing directoryScanner = this.connectorConfig.fileSystemListing();
        directoryScanner.setFilter(new CompositeFileListFilter(connectorConfig.fileSystemListingFilter()));

        final FileCleanupPolicy cleaner = connectorConfig.cleanupPolicy();
        final SourceOffsetPolicy offsetPolicy = connectorConfig.getSourceOffsetPolicy();

        try {
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
        } catch (Exception e) {
            LOG.error(
                    "Closing resources due to an error thrown during initialization of connector {} ",
                    connectorGroupName
            );
            closeSharedStateBackingStore();
            if (fsMonitorThread != null) fsMonitorThread.shutdown(0L);
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

        List<Map<String, String>> taskConfigs = new ArrayList<>(groupFiles.size());
        if (!groupFiles.isEmpty()) {
            final long taskConfigsGen = taskConfigsGeneration.getAndIncrement();
            for (List<String> group : groupFiles) {
                final Map<String, String> taskProps = new HashMap<>(configProperties);
                taskProps.put(TaskConfig.FILE_OBJECT_URIS_CONFIG, String.join(",", group));
                taskConfigs.add(taskProps);
            }
            for (int i = 0; i < groupFiles.size(); i++) {
                LOG.info(
                        "Created config for task_id={} with '{}' object files (task_config_gen={}).",
                        i,
                        groupFiles.get(i).size(),
                        taskConfigsGen);
            }
        } else {
            LOG.warn("Failed to create new task configs - no object files found.");
        }
        return taskConfigs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOG.info("Stopping connector");
        fsMonitorThread.shutdown();
        closeSharedStateBackingStore();
        try {
            fsMonitorThread.join(MAX_TIMEOUT);
        } catch (InterruptedException ignore) {
            LOG.info("Connector stopped");
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
            sharedStore.close();
        } catch (Exception exception) {
            LOG.error("Failed to shared StateBackingStore '{}'", connectorGroupName);
        }
    }
}