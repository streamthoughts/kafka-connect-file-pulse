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

import com.jsoniter.JsonIterator;
import com.jsoniter.output.EncodingMode;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsoniterSpi;
import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSerde;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.errors.SerializationException;

/**
 */
public class FileObjectSerde implements StateSerde<FileObject> {

    static {
        JsoniterSpi.registerTypeImplementation(FileObjectMeta.class, GenericFileObjectMeta.class);
        JsoniterSpi.registerTypeEncoder(URI.class, (obj, stream) -> stream.writeVal(obj.toString()));
        JsoniterSpi.registerTypeDecoder(URI.class, iter -> URI.create(iter.readString()));
        JsonStream.setMode(EncodingMode.REFLECTION_MODE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize(final FileObject object) {
        if (object == null) {
            return null;
        }
        try {
            String serialized = JsonStream.serialize(object);
            return serialized.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SerializationException("Failed to serialized object '" + object + "'", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileObject deserialize(byte[] data) {
        JsonIterator iterator = JsonIterator.parse(data);
        try {
            return iterator.read(FileObject.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}