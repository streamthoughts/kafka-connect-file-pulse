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
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.xml.XMLFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.xml.XMLFileInputReaderConfig;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputReader;
import io.streamthoughts.kafka.connect.filepulse.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.reader.ReaderException;
import io.streamthoughts.kafka.connect.filepulse.source.FileContext;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Map;

/**
 * This {@link FileInputReader} can be used for reading XML source files.
 */
public class LocalXMLFileInputReader extends BaseLocalFileInputReader {

    private XMLFileInputReaderConfig configs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        this.configs = new XMLFileInputReaderConfig(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileContext context,
                                                                     final IteratorManager iteratorManager) {

        final URI uri = context.metadata().uri();
        try {
            FileInputStream stream = new FileInputStream(new File(uri));
            return new XMLFileInputIterator(
                configs,
                iteratorManager,
                context,
                stream
            );
        } catch (FileNotFoundException e) {
            throw new ReaderException("Failed to open file: " + uri + ", file does not exist or is not accessible.");
        }
    }
}
