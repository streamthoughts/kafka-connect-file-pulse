/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.clean;

import io.streamthoughts.kafka.connect.filepulse.source.FileObject;
import java.util.List;

/**
 * Policy for cleaning a batch of completed source files.
 */
public interface BatchFileCleanupPolicy
        extends GenericFileCleanupPolicy<List<FileObject>, FileCleanupPolicyResultSet> {

}
