/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;

/**
 * A {@code FileInputIteratorFactory} can be used to get a new {@link FileInputIterator}.
 */
public interface FileInputIteratorFactory {

    /**
     * Creates a new {@link FileInputIterator} for the given object file.
     *
     * @param objectURI         the {@link URI} of the object file.
     * @return                  a new {@link FileInputIterator}.
     */
    FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI);
}
