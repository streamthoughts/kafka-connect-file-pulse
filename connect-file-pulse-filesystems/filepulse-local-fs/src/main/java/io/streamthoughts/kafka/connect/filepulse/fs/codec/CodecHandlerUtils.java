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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CodecHandlerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CodecHandlerUtils.class);

    private static final int DEFAULT_BYTES_BUFFER_SIZE = 1024;

    static void decompress(final InputStream inputStream,
                           final String parent,
                           final String name) throws IOException {

        final File file = new File(parent, name);

        if (!Files.exists(file.toPath())) {
            final String path = file.getAbsolutePath();
            LOG.debug("Decompressing file : {}", path);
            byte[] buffer = new byte[DEFAULT_BYTES_BUFFER_SIZE];

            try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file))) {
                int bytesCount;
                while ((bytesCount = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesCount);
                }
                LOG.debug("File decompressed successfully : {}", path);
            } catch (IOException e) {
                LOG.error("Unexpected error while decompressing file entry {} to directory {}", name, parent, e);
                throw e;
            }
        } else {
            LOG.debug("File already decompressed : {}", file.getName());
        }
    }
}
