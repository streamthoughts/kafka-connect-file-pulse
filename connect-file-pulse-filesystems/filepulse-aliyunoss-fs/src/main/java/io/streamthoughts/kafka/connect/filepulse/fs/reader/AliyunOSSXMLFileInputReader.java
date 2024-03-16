/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.xml.XMLFileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.xml.XMLFileInputReaderConfig;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.net.URI;
import java.util.Map;

/**
 * The {@code AliyunOSSXMLFileInputReader} can be used to created records from an XML file loaded from Aliyun OSS.
 */
public class AliyunOSSXMLFileInputReader extends BaseAliyunOSSInputReader {

    private XMLFileInputIteratorFactory factory;

    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);
        factory = new XMLFileInputIteratorFactory(new XMLFileInputReaderConfig(configs), storage(), iteratorManager());
    }

    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(final URI objectURI,
                                                                     final IteratorManager iteratorManager) {
        return factory.newIterator(objectURI);
    }
}
