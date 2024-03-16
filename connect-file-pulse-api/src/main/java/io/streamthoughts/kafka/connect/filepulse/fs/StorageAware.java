/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

public interface StorageAware {

    /**
     * Sets the {@link Storage}.
     *
     * @param storage the {@link Storage}.
     */
    void setStorage(final Storage storage);
}
