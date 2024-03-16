/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) StreamThoughts
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.streamthoughts.kafka.connect.filepulse.filter;

import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecordOffset;
import java.util.Map;
import org.apache.kafka.connect.header.ConnectHeaders;

/**
 * Default interface to expose contextual information to {@link RecordFilter}.
 */
public interface FilterContext {

    FileObjectMeta metadata();

    FileRecordOffset offset();

    ConnectHeaders headers();

    Long timestamp();

    Integer partition();

    String topic();

    String key();

    FilterError error();

    Map<String, Object> variables();
}
