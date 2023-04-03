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
package io.streamthoughts.kafka.connect.filepulse.utils;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MockFileCleaner implements FileCleanupPolicy {

    private final List<FileObjectMeta> succeed = new LinkedList<>();
    private final List<FileObjectMeta> failed = new LinkedList<>();

    private final boolean cleanUpReturn;

    public MockFileCleaner(boolean cleanUpReturn) {
        this.cleanUpReturn = cleanUpReturn;
    }

    public List<FileObjectMeta> getSucceed() {
        return succeed;
    }

    public List<FileObjectMeta> getFailed() {
        return failed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) { }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onSuccess(final FileObject source) {
        this.succeed.add(source.metadata());
        return cleanUpReturn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final FileObject source) {
        this.failed.add(source.metadata());
        return cleanUpReturn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {

    }
}