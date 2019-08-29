/*
 * Copyright 2019 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.scanner.local;

import org.apache.kafka.common.Configurable;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public interface FSDirectoryWalker extends Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    void configure(final Map<String, ?> configs);

    /**
     * Lists all files existing into the specified input directory.
     *
     * @param dir   the input directory to walk-through.
     * @return      the list of all files found.
     */
    Collection<File> listFiles(final File dir);

    /**
     * Sets the filter to apply on each file during directory listing.
     * @param filter    the filter to apply.
     */
    void setFilter(final FileListFilter filter);

}
