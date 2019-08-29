/*
 * Copyright 2019 StreamThoughts.
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

import static io.streamthoughts.kafka.connect.filepulse.config.TaskConfig.FILE_INPUT_PATHS_CONFIG;

import io.streamthoughts.kafka.connect.filepulse.Version;
import io.streamthoughts.kafka.connect.filepulse.config.ConnectorConfig;
import io.streamthoughts.kafka.connect.filepulse.offset.SimpleOffsetManager;
import io.streamthoughts.kafka.connect.filepulse.state.StateBackingStoreRegistry;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.scanner.FileSystemScanner;
import io.streamthoughts.kafka.connect.filepulse.scanner.LocalFileSystemScanner;
import io.streamthoughts.kafka.connect.filepulse.scanner.local.FSDirectoryWalker;
import io.streamthoughts.kafka.connect.filepulse.scanner.local.filter.CompositeFileListFilter;
import io.streamthoughts.kafka.connect.filepulse.state.FileStateBackingStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static final long MAX_TIMEOUT = 5000;

    private Map<String, String> configProperties;

    private AtomicInteger taskConfigsGeneration = new AtomicInteger(0);

    private FileSystemMonitorThread fsMonitorThread;

    private ConnectorConfig config;

    private FileSystemScanner scanner;

    private String groupId;

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
        LOG.info("Configuring connector");
        try {
            configProperties = props;
            config = new ConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't init FilePulseSourceConnector due to configuration error", e);
        }

        groupId = config.getTasksReporterGroupId();
        StateBackingStoreRegistry.instance().register(groupId, () -> {
            final Map<String, Object> configs = config.getInternalKafkaReporterConfig();
            return new FileStateBackingStore(
                    config.getTaskReporterTopic(),
                    groupId, configs);
        });

        final StateBackingStore<SourceFile> store = StateBackingStoreRegistry.instance().get(groupId);

        final FSDirectoryWalker directoryScanner = this.config.directoryScanner();
        directoryScanner.setFilter(new CompositeFileListFilter(config.filters()));

        scanner = new LocalFileSystemScanner(
                config.scanDirectoryPath(),
                directoryScanner,
                config.cleanupPolicy(),
                new SimpleOffsetManager(config.offsetStrategy()),
                store);

        fsMonitorThread = new FileSystemMonitorThread(context, scanner, config.scanInternalMs());
        fsMonitorThread.setUncaughtExceptionHandler((t, e) -> {
            LOG.info("Uncaught error from file system monitoring thread [{}]", t.getName(), e);
            throw new ConnectException(e);
        });
        fsMonitorThread.start();
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
        List<List<String>> groupFiles = scanner.partitionFilesAndGet(maxTasks);

        List<Map<String, String>> taskConfigs = new ArrayList<>(groupFiles.size());
        if (!groupFiles.isEmpty()) {
            final long taskConfigsGen = taskConfigsGeneration.getAndIncrement();
            for (List<String> group : groupFiles) {
                final Map<String, String> taskProps = new HashMap<>(configProperties);
                taskProps.put(FILE_INPUT_PATHS_CONFIG, String.join(",", group));
                taskConfigs.add(taskProps);
            }
            for(int i = 0; i < groupFiles.size(); i++) {
                LOG.info(
                    "Created config for task_id={} with '{}' source files (task_config_gen={}).",
                    i,
                    groupFiles.get(i).size(),
                    taskConfigsGen);
            }
        } else {
            LOG.warn("Failed to create new task configs - no source files found.");
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
        try {
            StateBackingStoreRegistry.instance().release(groupId);
            fsMonitorThread.join(MAX_TIMEOUT);
        } catch (InterruptedException ignore) {
        }
        LOG.info("Connector stopped");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigDef config() {
        return ConnectorConfig.getConf();
    }
}