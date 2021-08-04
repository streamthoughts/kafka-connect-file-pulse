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

import io.streamthoughts.kafka.connect.filepulse.fs.FileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.offset.DefaultSourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.source.DefaultTaskPartitioner;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.source.TaskPartitioner;
import io.streamthoughts.kafka.connect.filepulse.state.FileObjectStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStore;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CommonSourceConfig extends AbstractConfig {

    private static final String GROUP = "Common";

    public static final String OUTPUT_TOPIC_CONFIG = "topic";
    private static final String OUTPUT_TOPIC_DOC = "The Kafka topic to write the value to.";

    public static final String FS_LISTING_CLASS_CONFIG        = "fs.listing.class";
    private static final String FS_LISTING_CLASS_DOC          = "Class which is used to list eligible files from the scanned file system.";

    public static final String FS_LISTING_FILTERS_CONFIG      = "fs.listing.filters";
    private static final String FS_SCAN_FILTERS_DOC           = "Filters classes which are used to apply list input files.";

    public static final String TASKS_FILE_READER_CLASS_CONFIG = "tasks.reader.class";
    private static final String TASKS_FILE_READER_CLASS_DOC = "Class which is used by tasks to read an input file.";

    public static final String OFFSET_STRATEGY_CLASS_CONFIG = "offset.policy.class";
    private static final String OFFSET_STRATEGY_CLASS_DOC = "Class which is used to determine the source partition and offset that uniquely identify a input record";
    private static final String OFFSET_STRATEGY_CLASS_DEFAULT = DefaultSourceOffsetPolicy.class.getName();

    public static final String FILTERS_GROUP = "Filters";
    public static final String FILTER_CONFIG = "filters";
    private static final String FILTER_DOC = "List of filters aliases to apply on each value (order is important).";

    public static final String TASKS_FILE_STATUS_STORAGE_CLASS_CONFIG = "tasks.file.status.storage.class";
    public static final String TASKS_FILE_STATUS_STORAGE_CLASS_DOC = "The FileObjectStateBackingStore class to be used for storing status state of file objects.";

    public static final String TASK_PARTITIONER_CLASS_CONFIG = "task.partitioner.class";
    public static final String TASK_PARTITIONER_CLASS_DOC    = "The TaskPartitioner to be used for partitioning files to tasks";

    /**
     * Creates a new {@link CommonSourceConfig} instance.
     */
    public CommonSourceConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals, false);
    }

    public static ConfigDef getConfigDev() {
        int groupCounter = 0;
        return new ConfigDef()
                .define(
                        FS_LISTING_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        FS_LISTING_CLASS_DOC
                )

                .define(
                        FS_LISTING_FILTERS_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.MEDIUM,
                        FS_SCAN_FILTERS_DOC
                )
                .define(
                        TASKS_FILE_READER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        TASKS_FILE_READER_CLASS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_READER_CLASS_CONFIG
                )
                .define(
                        OUTPUT_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        OUTPUT_TOPIC_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        OUTPUT_TOPIC_CONFIG
                )
                .define(
                        OFFSET_STRATEGY_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        OFFSET_STRATEGY_CLASS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        OFFSET_STRATEGY_CLASS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        OFFSET_STRATEGY_CLASS_CONFIG
                )
                .define(
                        TASKS_FILE_STATUS_STORAGE_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        KafkaFileObjectStateBackingStore.class,
                        ConfigDef.Importance.LOW,
                        TASKS_FILE_STATUS_STORAGE_CLASS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_STATUS_STORAGE_CLASS_CONFIG
                )
                .define(
                        FILTER_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.HIGH,
                        FILTER_DOC,
                        FILTERS_GROUP,
                        -1,
                        ConfigDef.Width.NONE,
                        FILTER_CONFIG
                )
                .define(
                        TASK_PARTITIONER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DefaultTaskPartitioner.class,
                        ConfigDef.Importance.HIGH,
                        FILTER_DOC,
                        FILTERS_GROUP,
                        -1,
                        ConfigDef.Width.NONE,
                        TASK_PARTITIONER_CLASS_DOC
                );
    }


    public FileSystemListing<?> getFileSystemListing() {
        return getConfiguredInstance(FS_LISTING_CLASS_CONFIG, FileSystemListing.class);
    }

    public List<FileListFilter> getFileSystemListingFilter() {
        return getConfiguredInstances(FS_LISTING_FILTERS_CONFIG, FileListFilter.class);
    }

    public TaskPartitioner getTaskPartitioner() {
        return this.getConfiguredInstance(TASK_PARTITIONER_CLASS_CONFIG, TaskPartitioner.class);
    }

    public SourceOffsetPolicy getSourceOffsetPolicy() {
        return this.getConfiguredInstance(OFFSET_STRATEGY_CLASS_CONFIG, SourceOffsetPolicy.class);
    }

    public FileObjectStateBackingStore getStateBackingStore() {
        return this.getConfiguredInstance(
                TASKS_FILE_STATUS_STORAGE_CLASS_CONFIG,
                FileObjectStateBackingStore.class
        );
    }
}
