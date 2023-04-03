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