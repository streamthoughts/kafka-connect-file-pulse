/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.source;

/**
 * An object representing the position of a {@link FileRecord}.
 */
public interface FileRecordOffset {

    /**
     * Only for testing
     * @return  a {@link FileRecordOffset} throwing {@link UnsupportedOperationException}.
     */
    static FileRecordOffset invalid() {
        return () -> {
            throw new UnsupportedOperationException();
        };
    }

    FileObjectOffset toSourceOffset();

}