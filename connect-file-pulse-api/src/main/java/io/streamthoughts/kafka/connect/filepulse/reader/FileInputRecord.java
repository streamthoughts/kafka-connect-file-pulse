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
package io.streamthoughts.kafka.connect.filepulse.reader;

import io.streamthoughts.kafka.connect.filepulse.source.FileInputData;
import io.streamthoughts.kafka.connect.filepulse.source.FileInputOffset;

/**
 * Simple pair of {@link FileInputData} and {@link FileInputOffset}.
 */
public class FileInputRecord {

    private final FileInputData data;

    private final FileInputOffset offset;

    /**
     * Creates a new {@link FileInputRecord} instance.
     *
     * @param data the data value.
     * @param offset the data byte offset.
     */
    public FileInputRecord(final FileInputOffset offset, final FileInputData data) {
        this.data = data;
        this.offset = offset;
    }

    public FileInputOffset offset() {
        return offset;
    }

    public FileInputData data() {
        return data;
    }

    @Override
    public String toString() {
        return "[" +
                "data=" + data +
                ", offset=" + offset +
                ']';
    }
}
