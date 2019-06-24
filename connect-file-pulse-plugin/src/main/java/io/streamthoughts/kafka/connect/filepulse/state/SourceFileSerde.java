/*
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

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.streamthoughts.kafka.connect.filepulse.source.SourceFile;
import org.apache.kafka.common.errors.SerializationException;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSerde;

/**
 */
public class SourceFileSerde implements StateSerde<SourceFile> {

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize(final SourceFile state) {
        if (state == null) {
            return null;
        }

        String serialized = JsonStream.serialize(state);
        return serialized.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SourceFile deserialize(byte[] data) {
        JsonIterator iterator = JsonIterator.parse(data);
        try {
            return iterator.read(SourceFile.class);
        } catch (IOException e) {
            throw new SerializationException();
        }
    }
}