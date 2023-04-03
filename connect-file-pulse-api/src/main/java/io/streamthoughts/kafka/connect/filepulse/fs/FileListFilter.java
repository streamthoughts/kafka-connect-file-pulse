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
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * Interface which is used to filter files scanned from a file-system.
 */
public interface FileListFilter extends Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    default void configure(final Map<String, ?> config) {

    }

    /**
     * Apply the filters on the specified list of files.
     *
     * @param files files to filter
     * @return      a new filtered list of files.
     */
    Collection<FileObjectMeta> filterFiles(final Collection<FileObjectMeta> files);
}
