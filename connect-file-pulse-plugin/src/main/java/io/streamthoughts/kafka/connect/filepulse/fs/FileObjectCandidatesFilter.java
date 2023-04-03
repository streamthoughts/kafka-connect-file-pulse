/*
 * Copyright 2021 StreamThoughts.
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
package io.streamthoughts.kafka.connect.filepulse.fs;

import io.streamthoughts.kafka.connect.filepulse.config.SourceConnectorConfig;
import io.streamthoughts.kafka.connect.filepulse.internal.KeyValuePair;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectKey;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.SourceOffsetPolicy;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which is used to determinate if a list of file can be processed.
 */
public class FileObjectCandidatesFilter {

    private static final Logger LOG = LoggerFactory.getLogger(FileObjectCandidatesFilter.class);

    private final SourceOffsetPolicy offsetPolicy;

    private final Predicate<FileObjectKey> predicate;

    /**
     * Creates a new {@link FileObjectCandidatesFilter} instance.
     *
     * @param offsetPolicy  the {@link SourceOffsetPolicy} instance.
     */
    public FileObjectCandidatesFilter(final SourceOffsetPolicy offsetPolicy,
                                      final Predicate<FileObjectKey> predicate) {
        this.offsetPolicy = Objects.requireNonNull(offsetPolicy, "'offsetPolicy' should not be null");
        this.predicate = Objects.requireNonNull(predicate, "'snapshot' should not be null");
    }

    public static Map<FileObjectKey, FileObjectMeta> filter(final SourceOffsetPolicy offsetPolicy,
                                                            final Predicate<FileObjectKey> predicate,
                                                            final Collection<FileObjectMeta> candidates) {
        return new FileObjectCandidatesFilter(offsetPolicy, predicate).filter(candidates);
    }

    public Map<FileObjectKey, FileObjectMeta> filter(final Collection<FileObjectMeta> candidates) {

        final List<KeyValuePair<String, FileObjectMeta>> toScheduled = candidates.stream()
                .map(source -> KeyValuePair.of(offsetPolicy.toPartitionJson(source), source))
                .filter(kv -> predicate.test(FileObjectKey.of(kv.key)))
                .collect(Collectors.toList());

        // Looking for duplicates in object files, i.e., the OffsetPolicy generates two identical offsets for two files.
        final Stream<Map.Entry<String, List<KeyValuePair<String, FileObjectMeta>>>> entryStream = toScheduled
                .stream()
                .collect(Collectors.groupingBy(kv -> kv.key))
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().size() > 1);

        final Map<String, List<String>> duplicates = entryStream
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().map(m -> m.value.stringURI()).collect(Collectors.toList()))
                );

        if (!duplicates.isEmpty()) {
            final String formatted = duplicates
                    .entrySet()
                    .stream()
                    .map(e -> "partition_key=" + e.getKey() + ", files=" + e.getValue())
                    .collect(Collectors.joining("\n\t", "\n\t", "\n"));
            LOG.error(
                    "Duplicates object files detected. " +
                    "Consider changing the configuration for '{}'. " +
                    "Meanwhile all object files are ignored: {}",
                    SourceConnectorConfig.OFFSET_STRATEGY_CLASS_CONFIG,
                    formatted
            );
            return Collections.emptyMap(); // ignore all sources files
        }

        return toScheduled.stream().collect(Collectors.toMap(kv -> FileObjectKey.of(kv.key), kv -> kv.value));
    }
}
