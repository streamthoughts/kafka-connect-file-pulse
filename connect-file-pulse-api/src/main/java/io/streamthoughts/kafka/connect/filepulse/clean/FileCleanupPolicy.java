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

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import java.util.Map;

/**
 * Policy for cleaning individual completed source files.
 */
public interface FileCleanupPolicy extends
        GenericFileCleanupPolicy<FileObject, Boolean> {

    /**
     * {@inheritDoc}
     */
    @Override
    default void configure(final Map<String, ?> configs) { }

    /**
     * {@inheritDoc}
     */
    @Override
    default Boolean apply(final FileObject source) {
        final FileObjectStatus status = source.status();
        return isSuccess(status) ? onSuccess(source) : onFailure(source);
    }

    private boolean isSuccess(final FileObjectStatus status) {
        return status.equals(FileObjectStatus.COMPLETED) || status.equals(FileObjectStatus.COMMITTED);
    }

    boolean onSuccess(final FileObject source);

    boolean onFailure(final FileObject source);

}