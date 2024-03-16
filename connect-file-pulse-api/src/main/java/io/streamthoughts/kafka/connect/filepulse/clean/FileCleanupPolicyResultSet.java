/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
