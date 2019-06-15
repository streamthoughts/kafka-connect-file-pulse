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
package io.streamthoughts.kafka.connect.filepulse.source;


import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;

/**
 *
 */
public interface FileRecordsPollingConsumer<T> extends FileInputIterator<T> {

    /**
     * Returns the context for the last record return from the {@link #next()} method.
     * Can return {@code null} if the {@link #next()} method has never been invoke.
     *
     * @return  a {@link FileInputContext} instance.
     */
    FileInputContext context();

    /**
     * Sets a state listener.
     *
     * @param listener  the {@link StateListener} instance to be used.
     */
    void setFileListener(final StateListener listener);
}
