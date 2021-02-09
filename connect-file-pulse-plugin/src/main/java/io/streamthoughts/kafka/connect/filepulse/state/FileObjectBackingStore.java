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
package io.streamthoughts.kafka.connect.filepulse.state;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.storage.KafkaStateBackingStore;

import java.util.Map;

/**
 */
public class FileObjectBackingStore extends KafkaStateBackingStore<FileObject> {

    private static final String KEY_PREFIX = "connect-file-pulse";

    /**
     * Creates a new {@link FileObjectBackingStore} instance.
     *
     * @param store          the state store name.
     * @param groupId        the group attached to the backing store.
     * @param configs        the configuration.
     * @param isProducerOnly is the backing store only used for writing data.
     */
    public FileObjectBackingStore(final String store,
                                  final String groupId,
                                  final Map<String, ?> configs,
                                  final boolean isProducerOnly) {
        super(store, KEY_PREFIX, groupId, configs, new FileObjectSerde(), isProducerOnly);
    }
}