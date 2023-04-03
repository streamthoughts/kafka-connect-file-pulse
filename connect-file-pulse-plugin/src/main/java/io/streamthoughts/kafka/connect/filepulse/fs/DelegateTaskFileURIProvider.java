/*
 * Copyright 2021 StreamThoughts.
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

import io.streamthoughts.kafka.connect.filepulse.config.CommonSourceConfig;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import io.streamthoughts.kafka.connect.filepulse.source.TaskPartitioner;
import io.streamthoughts.kafka.connect.filepulse.state.StateBackingStoreAccess;
import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegateTaskFileURIProvider implements TaskFileURIProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DelegateTaskFileURIProvider.class);

    private static final Duration DEFAULT_REFRESH_TIMEOUT = Duration.ofSeconds(30);

    private static final String CONNECT_NAME_CONFIG = "name";

    private Config config;
    private FileSystemListing<?> fileSystemListing;
    private TaskPartitioner partitioner;
    private SourceOffsetPolicy sourceOffsetPolicy;

    private StateBackingStoreAccess sharedStore;

    private TaskFileOrder taskFileOrder;

    private StateSnapshot<FileObject> fileState;

    private boolean isFirstCall = true;

    /**
     * Creates a new {@link DelegateTaskFileURIProvider} instance.
     */
    public DelegateTaskFileURIProvider() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        config = new Config(configs);
        sharedStore = new StateBackingStoreAccess(
                configs.get(CONNECT_NAME_CONFIG).toString(),
                config::getStateBackingStore,
                true
        );

        partitioner = config.getTaskPartitioner();
        fileSystemListing = config.getFileSystemListing();
        sourceOffsetPolicy = config.getSourceOffsetPolicy();
        fileSystemListing.setFilter(new CompositeFileListFilter(config.getFileSystemListingFilter()));
        taskFileOrder = config.getTaskFilerOrder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<URI> nextURIs() {
        refreshState();
        final Collection<FileObjectMeta> filtered = FileObjectCandidatesFilter.filter(
                sourceOffsetPolicy,
                fileObjectKey -> {
                    final FileObject fileObject = fileState.getForKey(fileObjectKey.original());

                    if (fileObject == null)
                        return true;

                    final FileObjectStatus status = fileObject.status();

                    // If an object file is marked as COMPLETED (i.e. not COMMITTED) we should only consider
                    // it processable the first time this method is called.
                    if (status == FileObjectStatus.COMPLETED)
                        return isFirstCall;

                    // Otherwise, only return true if this object-file is not already done.
                    return !status.isDone();
                },
                fileSystemListing.listObjects()
        ).values();

        isFirstCall = false;

        List<FileObjectMeta> sorted = taskFileOrder.sort(filtered);
        return partitioner.partitionForTask(sorted, config.getTaskCount(), config.getTaskId());
    }

    private void refreshState() {
        try {
            final StateBackingStore<FileObject> store = sharedStore.get().getResource();
            store.refresh(DEFAULT_REFRESH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            fileState = store.snapshot();
        } catch (final TimeoutException e) {
            LOG.debug("Failed to reach end of states log quickly enough", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasMore() {
        return true;
    }

    public static final class Config extends CommonSourceConfig {

        public static final String TASK_ID_CONFIG = "task.id";
        private static final String TASK_ID_DOC = "The current task id";

        public static final String TASK_COUNT_CONFIG = "task.count";
        private static final String TASK_COUNT_DOC = "The total number tasks assigned.";

        /**
         * Creates a new {@link Config} instance.
         *
         * @param originals the original configs.
         */
        public Config(final Map<String, ?> originals) {
            super(getConf(), originals);
        }

        static ConfigDef getConf() {
            return CommonSourceConfig.getConfigDef()
                    .define(
                            TASK_ID_CONFIG,
                            ConfigDef.Type.INT,
                            ConfigDef.Importance.HIGH,
                            TASK_ID_DOC
                    )
                    .define(
                            TASK_COUNT_CONFIG,
                            ConfigDef.Type.INT,
                            ConfigDef.Importance.HIGH,
                            TASK_COUNT_DOC
                    );
        }

        public int getTaskId() {
            return this.getInt(TASK_ID_CONFIG);
        }

        public int getTaskCount() {
            return this.getInt(TASK_COUNT_CONFIG);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.info("Closing DelegateTaskFileURIProvider");
        closeSharedStateBackingStore();
        LOG.info("Closed DelegateTaskFileURIProvider");
    }

    private void closeSharedStateBackingStore() {
        try {
            if (sharedStore != null) {
                sharedStore.close();
            }
        } catch (Exception exception) {
            LOG.warn("Failed to close shared StateBackingStore. Error: '{}'", exception.getMessage());
        }
    }
}
