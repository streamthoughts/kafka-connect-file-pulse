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
package io.streamthoughts.kafka.connect.filepulse.config;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.common.config.ConfigDef;

public class SourceConnectorConfig extends CommonSourceConfig {

    /* Settings for DefaultFileSystemMonitor */
    public static final String ALLOW_TASKS_RECONFIG_AFTER_TIMEOUT_MS_CONFIG = "allow.tasks.reconfiguration.after.timeout.ms";
    private static final String ALLOW_TASKS_RECONFIG_AFTER_TIMEOUT_MS_DOC = "Specifies the timeout (in milliseconds) for the connector to allow tasks to be reconfigured when new files are detected, even if some tasks are still being processed.";

    public static final String FS_CLEANUP_POLICY_CLASS_CONFIG = "fs.cleanup.policy.class";
    private static final String FS_CLEANUP_POLICY_CLASS_DOC = "The class used to cleanup files that have been processed by tasks.";

    public static final String[] FS_CLEANUP_POLICY_EXECUTE_VALID_VALUES = {FileObjectStatus.COMPLETED.name(), FileObjectStatus.COMMITTED.name()};
    public static final String FS_CLEANUP_POLICY_EXECUTE_CONFIG = "fs.cleanup.policy.triggered.on";
    private static final String FS_CLEANUP_POLICY_EXECUTE_DOC = "Specify the status when a file get cleanup. Valid values are: " + Arrays.toString(FS_CLEANUP_POLICY_EXECUTE_VALID_VALUES);

    /* Settings for FileSystemMonitorThread */
    public static final String FS_LISTING_INTERVAL_MS_CONFIG  = "fs.listing.interval.ms";
    private static final String FS_LISTING_INTERVAL_MS_DOC    = "The time interval, in milliseconds, in which the connector invokes the scan of the filesystem.";
    private static final long FS_LISTING_INTERVAL_MS_DEFAULT  = 10000L;

    /* Settings for FilePulseSourceConnector */
    public static final String MAX_SCHEDULED_FILES_CONFIG     = "max.scheduled.files";
    private static final String MAX_SCHEDULED_FILES_DOC       = "Maximum number of files that can be schedules to tasks.";
    private static final int MAX_SCHEDULED_FILES_DEFAULT      = 1000;

    public static final String FS_LISTING_TASK_DELEGATION_ENABLED_CONFIG = "fs.listing.task.delegation.enabled";
    private static final String FS_LISTING_TASK_DELEGATION_ENABLED_DOC = "Boolean indicating whether the file listing process should be delegated to tasks.";

    /**
     * Creates a new {@link SourceConnectorConfig} instance.
     * @param originals the originals configuration.
     */
    public SourceConnectorConfig(final Map<?, ?> originals) {
        super(getConf(), originals);
    }

    public static ConfigDef getConf() {
        return CommonSourceConfig.getConfigDef()
                .define(
                        FS_LISTING_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        FS_LISTING_INTERVAL_MS_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        FS_LISTING_INTERVAL_MS_DOC
                )

                .define(
                        FS_CLEANUP_POLICY_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        FS_CLEANUP_POLICY_CLASS_DOC
                )

                .define(
                        ALLOW_TASKS_RECONFIG_AFTER_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        Long.MAX_VALUE,
                        ConfigDef.Importance.MEDIUM,
                        ALLOW_TASKS_RECONFIG_AFTER_TIMEOUT_MS_DOC
                )

                .define(
                        MAX_SCHEDULED_FILES_CONFIG,
                        ConfigDef.Type.INT,
                        MAX_SCHEDULED_FILES_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        MAX_SCHEDULED_FILES_DOC
                )

                .define(
                        FS_LISTING_TASK_DELEGATION_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        FS_LISTING_TASK_DELEGATION_ENABLED_DOC
                )
                .define(
                        FS_CLEANUP_POLICY_EXECUTE_CONFIG,
                        ConfigDef.Type.STRING,
                        FileObjectStatus.COMPLETED.name(),
                        ConfigDef.ValidString.in(FS_CLEANUP_POLICY_EXECUTE_VALID_VALUES),
                        ConfigDef.Importance.MEDIUM,
                        FS_CLEANUP_POLICY_EXECUTE_DOC
                );
    }

    public Long allowTasksReconfigurationAfterTimeoutMs() {
        return getLong(ALLOW_TASKS_RECONFIG_AFTER_TIMEOUT_MS_CONFIG);
    }

    public int getMaxScheduledFiles() {
        return getInt(MAX_SCHEDULED_FILES_CONFIG);
    }

    public FileCleanupPolicy getFsCleanupPolicy() {
        return getConfiguredInstance(FS_CLEANUP_POLICY_CLASS_CONFIG, FileCleanupPolicy.class);
    }

    public Predicate<FileObjectStatus> getFsCleanupPolicyPredicate() {
        return objetStatus -> {
            final String configureStatusString = getString(FS_CLEANUP_POLICY_EXECUTE_CONFIG);
            final FileObjectStatus configureStatus = FileObjectStatus.valueOf(configureStatusString);
            return objetStatus.isOneOf(FileObjectStatus.FAILED, configureStatus);
        };
    }

    public long getListingInterval() {
        return this.getLong(FS_LISTING_INTERVAL_MS_CONFIG);
    }

    public boolean isFileListingTaskDelegationEnabled() {
        return getBoolean(FS_LISTING_TASK_DELEGATION_ENABLED_CONFIG);
    }
}