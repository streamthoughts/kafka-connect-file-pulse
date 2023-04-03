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

import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG;
import static io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStoreConfig.TASKS_FILE_STATUS_STORAGE_NAME_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.Version;
import io.streamthoughts.kafka.connect.filepulse.config.SourceConnectorConfig;
import io.streamthoughts.kafka.connect.filepulse.config.SourceTaskConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.CompositeFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultFileSystemMonitor;
import io.streamthoughts.kafka.connect.filepulse.fs.DefaultTaskFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.fs.DelegateTaskFileURIProvider;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemMonitor;
import io.streamthoughts.kafka.connect.filepulse.state.StateBackingStoreAccess;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private SourceConnectorConfig connectorConfig;

    private FileSystemMonitor monitor;

    private String connectorGroupName;

    private TaskPartitioner partitioner;

    private StateBackingStoreAccess sharedStore;

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
            configProperties.put(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG, "true");
            connectorConfig = new SourceConnectorConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("Failed to initialize FilePulseSourceConnector due to configuration error", e);
        }

        try {

            sharedStore = new StateBackingStoreAccess(
                    connectorGroupName,
                    connectorConfig::getStateBackingStore,
                    false
            );

            partitioner = connectorConfig.getTaskPartitioner();

            final FileSystemListing<?> fileSystemListing = connectorConfig.getFileSystemListing();
            fileSystemListing.setFilter(new CompositeFileListFilter(connectorConfig.getFileSystemListingFilter()));

            monitor = new DefaultFileSystemMonitor(
                    connectorConfig.allowTasksReconfigurationAfterTimeoutMs(),
                    fileSystemListing,
                    connectorConfig.getFsCleanupPolicy(),
                    connectorConfig.getFsCleanupPolicyPredicate(),
                    connectorConfig.getSourceOffsetPolicy(),
                    sharedStore.get().getResource(),
                    connectorConfig.getTaskFilerOrder()
            );

            monitor.setFileSystemListingEnabled(!connectorConfig.isFileListingTaskDelegationEnabled());
            fsMonitorThread = new FileSystemMonitorThread(context, monitor, connectorConfig.getListingInterval());
            fsMonitorThread.setUncaughtExceptionHandler((t, e) -> {
                LOG.info("Uncaught error from file system monitoring thread [{}]", t.getName(), e);
                context.raiseError(new ConnectException("Unexpected error from FileSystemMonitorThread", e));
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

        if (connectorConfig.isFileListingTaskDelegationEnabled()) {
            final List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
            IntStream.range(0, maxTasks)
                    .forEachOrdered(i -> taskConfigs
                            .add(createTaskConfig(i, maxTasks, 0, null))
                    );
            return taskConfigs;
        }

        final List<List<String>> partitioned = partitionAndGet(maxTasks);

        final long taskConfigsGen = taskConfigsGeneration.getAndIncrement();
        final List<Map<String, String>> taskConfigs = new ArrayList<>(partitioned.size());

        if (partitioned.isEmpty()) {
            if (taskConfigsGen > 0) {
                LOG.info("No object file was found - skip task reconfiguration.");
                return taskConfigs;
            }
            LOG.info("No object file was found - resetting all tasks with an empty config.");
            IntStream.range(0, maxTasks)
                    .forEachOrdered(i -> taskConfigs
                            .add(createTaskConfig(i, maxTasks, taskConfigsGen, Collections.emptyList()))
                    );
        } else {
            IntStream.range(0, partitioned.size())
                    .forEachOrdered(i -> taskConfigs
                            .add(createTaskConfig(i, partitioned.size(), taskConfigsGen, partitioned.get(i)))
                    );
        }

        IntStream.range(0, partitioned.size()).forEachOrdered(i -> LOG.info(
                "Created config for task_id={} with '{}' object files (task_config_gen={}).",
                i,
                partitioned.get(i).size(),
                taskConfigsGen)
        );
        return taskConfigs;
    }

    private List<List<String>> partitionAndGet(int maxTasks) {
        final List<FileObjectMeta> files = monitor.listFilesToSchedule(connectorConfig.getMaxScheduledFiles());
        return partitioner.partition(files, maxTasks)
                .stream()
                .map(it -> it.stream().map(Object::toString).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    private Map<String, String> createTaskConfig(final int taskId,
                                                 final int taskCount,
                                                 final long taskConfigGen,
                                                 final List<String> uris) {
        final Map<String, String> taskConfig = new HashMap<>(configProperties);
        taskConfig.put(SourceTaskConfig.TASK_GENERATION_ID, String.valueOf(taskConfigGen));
        if (connectorConfig.isFileListingTaskDelegationEnabled()) {
            taskConfig.put(SourceTaskConfig.FILE_URIS_PROVIDER_CONFIG, DelegateTaskFileURIProvider.class.getName());
            taskConfig.put(SourceTaskConfig.TASK_PARTITIONER_CLASS_CONFIG, HashByURITaskPartitioner.class.getName());
            taskConfig.put(DelegateTaskFileURIProvider.Config.TASK_ID_CONFIG, String.valueOf(taskId));
            taskConfig.put(DelegateTaskFileURIProvider.Config.TASK_COUNT_CONFIG, String.valueOf(taskCount));
            taskConfig.put(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG, "true");
        } else {
            taskConfig.put(SourceTaskConfig.FILE_URIS_PROVIDER_CONFIG, DefaultTaskFileURIProvider.class.getName());
            taskConfig.put(DefaultTaskFileURIProvider.Config.FILE_OBJECT_URIS_CONFIG, String.join(",", uris));
            taskConfig.put(TASKS_FILE_STATUS_STORAGE_CONSUMER_ENABLED_CONFIG, "false");
        }
        return taskConfig;
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
                    try {
                    fsMonitorThread.shutdown(DEFAULT_MAX_TIMEOUT);
                    fsMonitorThread.join(DEFAULT_MAX_TIMEOUT);
                    } catch (InterruptedException e) {
                        LOG.warn("Failed to close file-system monitoring thread. Error: {}", e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
                if (partitioner != null) {
                    try {
                        partitioner.close();
                    } catch (final Exception e) {
                        LOG.warn("Failed to close TaskPartition. Error: {}", e.getMessage());
                    }
                }
            }
            finally {
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
        return SourceConnectorConfig.getConf();
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