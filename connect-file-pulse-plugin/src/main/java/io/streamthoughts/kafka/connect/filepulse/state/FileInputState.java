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

import com.jsoniter.annotation.JsonCreator;
import com.jsoniter.annotation.JsonProperty;
import io.streamthoughts.kafka.connect.filepulse.offset.SourceMetadata;
import io.streamthoughts.kafka.connect.filepulse.offset.SourceOffset;

import java.util.Arrays;
import java.util.Objects;

/**
 *
 */
public class FileInputState {

    public enum Status {
        /**
         * The file has been scheduled by the connector thread.
         */
        SCHEDULED,

        /**
         * The file can't be scheduled because it is not readable.
         */
        INVALID,

        /**
         * The file is starting to be processed by a task.
         */
        STARTED,

        /**
         * The is file is currently being read by a task.
         */
        READING,

        /**
         * The file processing is completed.
         */
        COMPLETED,

        /**
         * The file processing failed.
         */
        FAILED,

        /**
         * The file has been successfully clean up (depending of the configured strategy).
         */
        CLEANED;

        public static Status[] started() {
            return new Status[]{SCHEDULED, STARTED, READING};
        }

        public static Status[] completed() {
            return new Status[]{COMPLETED, FAILED};
        }

        public boolean isOneOf(final Status...states) {
            return Arrays.asList(states).contains(this);
        }

    }

    private final String hostname;
    private final Status status;
    private final SourceMetadata metadata;
    private final SourceOffset offset;

    /**
     * Creates a new {@link FileInputState} instance.
     *
     * @param hostname  the hostname.
     * @param status    the file status.
     * @param metadata  the file metadata.
     * @param offset    the file startPosition.
     */
    @JsonCreator
    public FileInputState(@JsonProperty("hostname") final String hostname,
                          @JsonProperty("status") final Status status,
                          @JsonProperty("metadata") final SourceMetadata metadata,
                          @JsonProperty("offset") final SourceOffset offset) {
        Objects.requireNonNull(metadata, "metadata can't be null");
        Objects.requireNonNull(offset, "offset can't be null");
        this.hostname = hostname;
        this.status = status;
        this.metadata = metadata;
        this.offset = offset;
    }

    public String hostname() {
        return hostname;
    }

    public Status status() {
        return status;
    }

    public SourceMetadata metadata() {
        return metadata;
    }

    public SourceOffset offset() {
        return offset;
    }

    public FileInputState withState(final Status status) {
        Objects.requireNonNull(status, "status can't be null");
        return new FileInputState(hostname, status, metadata, offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileInputState)) return false;
        FileInputState that = (FileInputState) o;
        return Objects.equals(hostname, that.hostname) &&
                status == that.status &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, status, metadata, offset);
    }

    @Override
    public String toString() {
        return "[" +
                "hostname='" + hostname + '\'' +
                ", status=" + status +
                ", metadata=" + metadata +
                ", offset=" + offset +
                ']';
    }
}