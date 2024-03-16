/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs.reader.avro;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.internal.Silent;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import java.io.File;
import java.io.FileInputStream;

public class AvroDataStreamIteratorTest extends BaseAvroDataIteratorTest {

    /**
     * {@inheritDoc}
     */
    @Override
    FileInputIterator<FileRecord<TypedStruct>> newIterator(final FileObjectMeta objectMeta) {
        return Silent.unchecked(() -> new AvroDataStreamIterator(
                objectMeta,
                new IteratorManager(),
                new FileInputStream(new File(objectMeta.uri())))
        );
    }
}