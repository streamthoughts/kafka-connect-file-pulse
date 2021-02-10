/*
 * Copyright 2019-2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.reader.AbstractFileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class BaseLocalFileInputReader extends AbstractFileInputReader {

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObjectMeta readMetadata(final URI fileURI) {
        return new LocalFileObjectMeta(new File(fileURI));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadable(final URI fileURI) {
        return Files.isReadable(Paths.get(fileURI));
    }
}
