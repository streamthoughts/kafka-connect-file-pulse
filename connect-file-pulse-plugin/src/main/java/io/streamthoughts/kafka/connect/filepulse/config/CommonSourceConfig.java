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

import com.jsoniter.JsonIterator;
import io.streamthoughts.kafka.connect.filepulse.avro.AvroSchemaConverter;
import io.streamthoughts.kafka.connect.filepulse.fs.FileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.fs.TaskFileOrder;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import io.streamthoughts.kafka.connect.filepulse.offset.DefaultSourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.source.DefaultTaskPartitioner;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.source.TaskPartitioner;
import io.streamthoughts.kafka.connect.filepulse.state.FileObjectStateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.state.KafkaFileObjectStateBackingStore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;

/**
 *
 */
public class CommonSourceConfig extends AbstractConfig {

    private static final String GROUP = "Common";

    public static final String OUTPUT_TOPIC_CONFIG = "topic";
    private static final String OUTPUT_TOPIC_DOC = "The Kafka topic to write the value to.";

    public static final String FS_LISTING_CLASS_CONFIG = "fs.listing.class";
    private static final String FS_LISTING_CLASS_DOC = "Class which is used to list eligible files from the scanned file system.";

    public static final String FS_LISTING_FILTERS_CONFIG = "fs.listing.filters";
    private static final String FS_SCAN_FILTERS_DOC = "Filters classes which are used to apply list input files.";

    public static final String TASKS_FILE_READER_CLASS_CONFIG = "tasks.reader.class";
    private static final String TASKS_FILE_READER_CLASS_DOC = "Class which is used by tasks to read an input file.";

    public static final String TASKS_FILE_PROCESSING_ORDER_BY_CONFIG = "tasks.file.processing.order.by";
    private static final String TASKS_FILE_PROCESSING_ORDER_BY_DOC = "The strategy to be used for sorting files for processing. Valid values are: LAST_MODIFIED, URI, CONTENT_LENGTH, CONTENT_LENGTH_DESC.";

    public static final String TASKS_HALT_ON_ERROR_CONFIG = "tasks.halt.on.error";
    private static final String TASKS_HALT_ON_ERROR_DOC = "Should a task halt when it encounters an error or continue to the next file.";

    public static final String TASKS_EMPTY_POLL_WAIT_MS_CONFIG = "tasks.empty.poll.wait.ms";
    public static final String TASKS_EMPTY_POLL_WAIT_MS_DOC = "The amount of time in millisecond a tasks should wait if a poll returns an empty list of records.";

    public static final String OFFSET_STRATEGY_CLASS_CONFIG = "offset.policy.class";
    private static final String OFFSET_STRATEGY_CLASS_DOC = "Class which is used to determine the source partition and offset that uniquely identify a input record";
    private static final String OFFSET_STRATEGY_CLASS_DEFAULT = DefaultSourceOffsetPolicy.class.getName();

    public static final String FILTERS_GROUP = "Filters";
    public static final String FILTER_CONFIG = "filters";
    private static final String FILTER_DOC = "List of filters aliases to apply on each value (order is important).";

    public static final String TASKS_FILE_STATUS_STORAGE_CLASS_CONFIG = "tasks.file.status.storage.class";
    private static final String TASKS_FILE_STATUS_STORAGE_CLASS_DOC = "The FileObjectStateBackingStore class to be used for storing status state of file objects.";

    public static final String TASK_PARTITIONER_CLASS_CONFIG = "task.partitioner.class";
    private static final String TASK_PARTITIONER_CLASS_DOC = "The TaskPartitioner to be used for partitioning files to tasks";

    public static final String RECORD_VALUE_SCHEMA_CONFIG = "value.connect.schema";
    private static final String RECORD_VALUE_SCHEMA_DOC = "The schema for the record-value";

    public static final String RECORD_VALUE_SCHEMA_TYPE_CONFIG = "value.connect.schema.type";
    private static final String RECORD_VALUE_SCHEMA_TYPE_DOC = "The type of the schema passed through 'value.connect.schema' (supported values are: 'CONNECT', 'AVRO')";

    public static final String RECORD_VALUE_SCHEMA_CONDITION_TOPIC_PATTERN_CONFIG = "value.connect.schema.condition.topic.pattern";

    private static final String RECORD_VALUE_SCHEMA_CONDITION_TOPIC_PATTERN_DOC =
            "Specify the Java regular expression pattern to match topic for which value schema must be applied";

    public static final String RECORD_VALUE_SCHEMA_MERGE_ENABLE_CONFIG = "merge.value.connect.schemas";
    private static final String RECORD_VALUE_SCHEMA_MERGE_ENABLE_DOC = "Specify if schemas deriving from record-values should be recursively merged. " +
            "If set to true, then schemas deriving from a record will be merged with the schema of the last produced record. " +
            "If `value.connect.schema` is set, then the provided schema will be merged with the schema deriving from the first generated record.";

    private static final String CONNECT_SCHEMA_KEEP_LEADING_UNDERSCORES_ON_FIELD_NAME_CONFIG = "connect.schema.keep.leading.underscores.on.record.name";
    private static final String CONNECT_SCHEMA_KEEP_LEADING_UNDERSCORES_ON_FIELD_NAME_DOC = "Specify if leading underscores should be kept when generating schema record name.";

    /**
     * Creates a new {@link CommonSourceConfig} instance.
     */
    public CommonSourceConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals, false);
    }

    public static ConfigDef getConfigDef() {
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
                        TASKS_HALT_ON_ERROR_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        TASKS_HALT_ON_ERROR_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_HALT_ON_ERROR_CONFIG
                )
                .define(
                        TASKS_EMPTY_POLL_WAIT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        500,
                        ConfigDef.Importance.LOW,
                        TASKS_EMPTY_POLL_WAIT_MS_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_EMPTY_POLL_WAIT_MS_CONFIG
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
                        RECORD_VALUE_SCHEMA_CONDITION_TOPIC_PATTERN_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        RECORD_VALUE_SCHEMA_CONDITION_TOPIC_PATTERN_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        RECORD_VALUE_SCHEMA_CONDITION_TOPIC_PATTERN_CONFIG
                )
                .define(
                        RECORD_VALUE_SCHEMA_TYPE_CONFIG,
                        ConfigDef.Type.STRING,
                        ConnectSchemaType.CONNECT.name(),
                        ConfigDef.Importance.MEDIUM,
                        RECORD_VALUE_SCHEMA_TYPE_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        RECORD_VALUE_SCHEMA_TYPE_CONFIG
                )
                .define(
                        RECORD_VALUE_SCHEMA_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        RECORD_VALUE_SCHEMA_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        RECORD_VALUE_SCHEMA_CONFIG
                )
                .define(
                        RECORD_VALUE_SCHEMA_MERGE_ENABLE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        RECORD_VALUE_SCHEMA_MERGE_ENABLE_DOC,
                        GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        RECORD_VALUE_SCHEMA_MERGE_ENABLE_CONFIG
                )
                .define(
                        FILTER_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.HIGH,
                        FILTER_DOC,
                        FILTERS_GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        FILTER_CONFIG
                )
                .define(
                        TASK_PARTITIONER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DefaultTaskPartitioner.class,
                        ConfigDef.Importance.HIGH,
                        FILTER_DOC,
                        TASK_PARTITIONER_CLASS_DOC,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASK_PARTITIONER_CLASS_CONFIG
                )
                .define(
                        TASKS_FILE_PROCESSING_ORDER_BY_CONFIG,
                        ConfigDef.Type.STRING,
                        TaskFileOrder.BuiltIn.LAST_MODIFIED.name(),
                        ConfigDef.Importance.MEDIUM,
                        TASKS_FILE_PROCESSING_ORDER_BY_DOC,
                        FILTERS_GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        TASKS_FILE_PROCESSING_ORDER_BY_CONFIG
                )
                .define(
                        CONNECT_SCHEMA_KEEP_LEADING_UNDERSCORES_ON_FIELD_NAME_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        CONNECT_SCHEMA_KEEP_LEADING_UNDERSCORES_ON_FIELD_NAME_DOC,
                        FILTERS_GROUP,
                        groupCounter++,
                        ConfigDef.Width.NONE,
                        CONNECT_SCHEMA_KEEP_LEADING_UNDERSCORES_ON_FIELD_NAME_CONFIG
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

    public TaskFileOrder getTaskFilerOrder() {
        return TaskFileOrder.findBuiltInByName(this.getString(TASKS_FILE_PROCESSING_ORDER_BY_CONFIG));
    }

    public boolean isTaskHaltOnError() {
        return this.getBoolean(TASKS_HALT_ON_ERROR_CONFIG);
    }

    public long getTaskEmptyPollWaitMs() {
        return this.getLong(TASKS_EMPTY_POLL_WAIT_MS_CONFIG);
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

    public String getValueSchemaConditionTopicPattern() {
        return getString(RECORD_VALUE_SCHEMA_CONDITION_TOPIC_PATTERN_CONFIG);
    }

    public Schema getValueConnectSchema() {
        String valueConnectSchemaTypeString = getString(RECORD_VALUE_SCHEMA_TYPE_CONFIG);
        ConnectSchemaType schemaType = ConnectSchemaType.getForNameIgnoreCase(valueConnectSchemaTypeString);
        switch (schemaType) {
            case CONNECT:
                return readConfigSchema();
            case AVRO:
                return readAvroSchema();
            case INVALID:
            default:
                throw new ConfigException(
                        "Unsupported or invalid value for '"
                                + RECORD_VALUE_SCHEMA_TYPE_CONFIG + "' , was "
                                + valueConnectSchemaTypeString);
        }
    }

    public boolean isValueConnectSchemaMergeEnabled() {
        return getBoolean(RECORD_VALUE_SCHEMA_MERGE_ENABLE_CONFIG);
    }

    public boolean isSchemaKeepLeadingUnderscoreOnFieldName() {
        return getBoolean(CONNECT_SCHEMA_KEEP_LEADING_UNDERSCORES_ON_FIELD_NAME_CONFIG);
    }

    private Schema readAvroSchema() {
        try {
            final String schema = getString(CommonSourceConfig.RECORD_VALUE_SCHEMA_CONFIG);
            if (StringUtils.isBlank(schema)) return null;

            AvroSchemaConverter converter = new AvroSchemaConverter();
            return converter.toConnectSchema(schema);
        } catch (Exception e) {
            throw new ConfigException(
                    "Failed to read avro-schema for '" + CommonSourceConfig.RECORD_VALUE_SCHEMA_CONFIG + "'", e);
        }
    }

    private Schema readConfigSchema() {
        final String schema = getString(CommonSourceConfig.RECORD_VALUE_SCHEMA_CONFIG);
        if (StringUtils.isBlank(schema)) return null;

        try {
            return JsonIterator.deserialize(schema, ConfigSchema.class).get();
        } catch (Exception e) {
            throw new ConfigException(
                    "Failed to read connect-schema for '" + CommonSourceConfig.RECORD_VALUE_SCHEMA_CONFIG + "'", e);
        }
    }
}
