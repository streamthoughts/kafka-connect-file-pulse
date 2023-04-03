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
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DelegateBatchFileCleanupPolicy implements BatchFileCleanupPolicy {

    private final FileCleanupPolicy delegate;

    /**
     * Creates a new {@link DelegateBatchFileCleanupPolicy} instance.
     *
     * @param delegate  the policy to be executed.
     */
    public DelegateBatchFileCleanupPolicy(final FileCleanupPolicy delegate) {
        Objects.requireNonNull(delegate, "delegate cannot be null");
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        delegate.configure(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileCleanupPolicyResultSet apply(final List<FileObject> sources) {
        FileCleanupPolicyResultSet rs = new FileCleanupPolicyResultSet();
        for (FileObject source : sources) {
            if (delegate.apply(source)) {
                rs.add(source, FileCleanupPolicyResult.SUCCEED);
            } else {
                rs.add(source, FileCleanupPolicyResult.FAILED);
            }
        }
        return rs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        delegate.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(final Storage storage) {
        delegate.setStorage(storage);
    }
}
