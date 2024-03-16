/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.state;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectOffset;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectStatus;
import io.streamthoughts.kafka.connect.filepulse.source.LocalFileObjectMeta;
import java.io.IOException;
import java.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 */
public class FileObjectSerdeTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void should_serialize_and_deserialize_given_valid_object() throws IOException {
        FileObjectSerde serde = new FileObjectSerde();

        FileObject state = new FileObject(
            new LocalFileObjectMeta(testFolder.newFile()),
            new FileObjectOffset(10L, 10L, Instant.now().getEpochSecond()),
            FileObjectStatus.STARTED
        );
        byte[] bytes = serde.serialize(state);
        Assert.assertEquals(serde.deserialize(bytes), state);
    }

}