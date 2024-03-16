/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse;

import io.streamthoughts.kafka.connect.filepulse.fs.FileListFilter;
import io.streamthoughts.kafka.connect.filepulse.fs.FileSystemListing;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.Map;

public class MockFileSystemListing implements FileSystemListing {

    @Override
    public void configure(Map configs) {

    }

    @Override
    public Collection<FileObjectMeta> listObjects() {
        return null;
    }

    @Override
    public void setFilter(FileListFilter filter) {

    }

    @Override
    public Storage storage() {
        return null;
    }
}
