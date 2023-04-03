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

import io.streamthoughts.kafka.connect.filepulse.internal.KeyValuePair;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.function.BiConsumer;

public class FileCleanupPolicyResultSet {

    private final Collection<KeyValuePair<FileObject, FileCleanupPolicyResult>> resultSet;

    /**
     * Creates a new {@link FileCleanupPolicyResultSet} instance.
     */
    public FileCleanupPolicyResultSet() {
        this.resultSet = new LinkedList<>();
    }


    public void add(final FileObject source, FileCleanupPolicyResult result) {
        Objects.requireNonNull(source, "source cannot be null");
        Objects.requireNonNull(result, "result cannot be null");
        resultSet.add(KeyValuePair.of(source, result));
    }

    public void forEach(final BiConsumer<? super FileObject, FileCleanupPolicyResult> action) {
        Objects.requireNonNull(action);
        for (KeyValuePair<FileObject, FileCleanupPolicyResult> kv : resultSet) {
            action.accept(kv.key, kv.value);
        }
    }

}
