/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.clean;

import io.streamthoughts.kafka.connect.filepulse.clean.FileCleanupPolicy;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;

/**
 * Policy for deleting completed files.
 */
public class DeleteCleanupPolicy implements FileCleanupPolicy {

    private Storage storage;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onSuccess(final FileObject source) {
        return storage.delete(source.metadata().uri());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onFailure(final FileObject source) {
        return storage.delete(source.metadata().uri());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() { }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {
        this.storage = storage;
    }
}