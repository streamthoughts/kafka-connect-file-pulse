/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
