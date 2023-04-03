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
package io.streamthoughts.kafka.connect.filepulse.fs.codec;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.Configurable;

public interface CodecHandler extends Configurable {

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    void configure(Map<String, ?> configs);

    /**
     * Check whether this codec can decompress the specified file.
     *
     * @param file  the file to check.
     * @return  <code>true</code> if the file is compressed and is supported.
     */
    boolean canRead(final File file);

    /**
     * Decompress the specified input files.
     *
     * @param file      the input file to decompress
     * @return          the file pointer to the decompress directory.
     *
     * @throws IOException if an error occurred while decompressing the file.
     */
    File decompress(final File file) throws IOException;
}
